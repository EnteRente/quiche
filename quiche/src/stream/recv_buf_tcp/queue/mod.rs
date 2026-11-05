// Copyright (C) 2026, Cloudflare, Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
// IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
// THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
// PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

use std::collections::VecDeque;
use std::fmt;
use std::mem;
use std::sync::Mutex;

use intrusive_collections::intrusive_adapter;
use intrusive_collections::rbtree::CursorMut;
use intrusive_collections::rbtree::CursorOwning;
use intrusive_collections::Bound::Included;
use intrusive_collections::KeyAdapter;
use intrusive_collections::RBTree;
use intrusive_collections::RBTreeAtomicLink;
use smallvec::SmallVec;

use crate::range_buf::RangeBuf;

use super::MAX_FRAGMENTS;

#[derive(Debug)]
pub(super) struct Segment {
    link: RBTreeAtomicLink,
    pub(super) off: u64,
    pub(super) end: u64,
    pub(super) bufs: SmallVec<[RangeBuf; 1]>,
}

impl Segment {
    fn new(buf: RangeBuf) -> Box<Self> {
        Box::new(Self {
            link: RBTreeAtomicLink::new(),
            off: buf.off(),
            end: buf.max_off(),
            bufs: smallvec::smallvec![buf],
        })
    }

    fn can_coalesce(&self, buf: &RangeBuf) -> bool {
        self.end == buf.off() && self.bufs.len() < MAX_FRAGMENTS
    }

    fn coalesce(&mut self, buf: RangeBuf) {
        debug_assert!(self.can_coalesce(&buf));
        self.end = buf.max_off();
        self.bufs.push(buf);
    }
}

intrusive_adapter!(SegmentAdapter = Box<Segment>: Segment {
    link => RBTreeAtomicLink
});

impl<'a> KeyAdapter<'a> for SegmentAdapter {
    type Key = u64;

    fn get_key(&self, segment: &'a Segment) -> u64 {
        segment.off
    }
}

/// The rightmost segment is owned separately so it can be coalesced without
/// unsafe pointer access or interior mutability. Earlier segments are linked
/// into the tree, whose owning cursor caches its rightmost linked node.
/// All tree mutations use the crate's safe cursor API.
pub(super) struct OutOfOrder {
    // CursorOwning is Send but not Sync. Mutex supplies Sync while exposing
    // the cursor only through get_mut(), which does not acquire a lock.
    // The cursor points at the rightmost linked node, or null for an empty
    // tree. Slow paths restore this invariant before returning.
    tree: Mutex<CursorOwning<SegmentAdapter>>,
    tail: Option<Box<Segment>>,
    first_off: Option<u64>,
}

impl Default for OutOfOrder {
    fn default() -> Self {
        Self {
            tree: Mutex::new(RBTree::default().cursor_owning()),
            tail: None,
            first_off: None,
        }
    }
}

impl fmt::Debug for OutOfOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Do not acquire the mutex just to format the linked tree.
        f.debug_struct("OutOfOrder")
            .field("tail", &self.tail)
            .field("first_off", &self.first_off)
            .finish_non_exhaustive()
    }
}

impl OutOfOrder {
    pub(super) fn first_off(&self) -> Option<u64> {
        self.first_off
    }

    pub(super) fn clear(&mut self) {
        *self.tree.get_mut().unwrap() = RBTree::default().cursor_owning();
        self.tail = None;
        self.first_off = None;
    }

    fn append_linked(&mut self, segment: Box<Segment>) {
        self.tree.get_mut().unwrap().with_cursor_mut(|cursor| {
            debug_assert!(cursor
                .get()
                .is_none_or(|last| last.end <= segment.off));

            // The cached node has no right child. insert_after links the new
            // maximum directly, without a lookup from the root. The new leaf
            // is its immediate successor; advance the cache to that leaf.
            // Red-black balancing still takes O(log n) in the worst case.
            cursor.insert_after(segment);
            cursor.move_next();
        });
    }

    fn take_tree(&mut self) -> RBTree<SegmentAdapter> {
        mem::replace(
            self.tree.get_mut().unwrap(),
            RBTree::default().cursor_owning(),
        )
        .into_inner()
    }

    fn restore_tree(&mut self, tree: RBTree<SegmentAdapter>) {
        // General insertion and promotion may change the rightmost linked
        // node. Only these slow paths need to find it from the root again.
        *self.tree.get_mut().unwrap() = tree.back_owning();
    }

    pub(super) fn write(&mut self, mut buf: RangeBuf) {
        let Some(tail) = self.tail.as_mut() else {
            debug_assert!(self.tree.get_mut().unwrap().as_cursor().is_null());
            self.first_off = Some(buf.off());
            self.tail = Some(Segment::new(buf));
            return;
        };

        if buf.off() >= tail.off {
            if buf.max_off() <= tail.end {
                return;
            }
            if buf.off() < tail.end {
                buf.consume((tail.end - buf.off()) as usize);
            }
            if tail.can_coalesce(&buf) {
                // The common contiguous tail path needs no tree traversal,
                // node allocation, or payload copy.
                tail.coalesce(buf);
            } else {
                let previous = self.tail.replace(Segment::new(buf)).unwrap();
                self.append_linked(previous);
            }
            return;
        }

        let overlaps_tail = buf.max_off() > tail.off;
        self.first_off = Some(self.first_off.unwrap().min(buf.off()));

        // Most reordered ranges belong entirely before the tail. Only link
        // it when resolving an overlap that actually reaches its payload.
        if overlaps_tail {
            let tail = self.tail.take().unwrap();
            self.append_linked(tail);
        }
        let mut tree = self.take_tree();
        Self::insert_in_tree(&mut tree, buf);
        if overlaps_tail {
            self.tail = tree.back_mut().remove();
        }
        self.restore_tree(tree);
    }

    fn insert_in_tree(tree: &mut RBTree<SegmentAdapter>, mut buf: RangeBuf) {
        let mut cursor = tree.upper_bound_mut(Included(&buf.off()));
        loop {
            if let Some(prev) = cursor.get() {
                if prev.end >= buf.max_off() {
                    break;
                }
                if prev.end > buf.off() {
                    buf.consume((prev.end - buf.off()) as usize);
                }
            }

            let next_off = cursor.peek_next().get().map(|next| next.off);
            if next_off.is_none_or(|off| off >= buf.max_off()) {
                Self::insert_after(&mut cursor, buf);
                break;
            }
            let next_off = next_off.unwrap();
            if next_off > buf.off() {
                // split_off indexes from the original view, not its current
                // read position. Splitting only clones the payload handle.
                let at = buf.pos - buf.start + (next_off - buf.off()) as usize;
                let rest = buf.split_off(at);
                Self::insert_after(&mut cursor, buf);
                buf = rest;
            }
            // Retain already buffered bytes, including conflicting overlaps.
            cursor.move_next();
        }
    }

    fn insert_after(cursor: &mut CursorMut<'_, SegmentAdapter>, buf: RangeBuf) {
        if cursor.get().is_some_and(|prev| prev.can_coalesce(&buf)) {
            // Linked values are shared references. Take ownership to mutate
            // the payload descriptors, then reinsert at the same position.
            let mut prev = cursor.remove().unwrap();
            prev.coalesce(buf);
            cursor.insert_before(prev);
            cursor.move_prev();
        } else {
            cursor.insert_after(Segment::new(buf));
            cursor.move_next();
        }
    }

    pub(super) fn promote(
        &mut self, ready: &mut VecDeque<RangeBuf>, contiguous_off: &mut u64,
    ) {
        if self.first_off != Some(*contiguous_off) {
            return;
        }
        let mut tree = self.take_tree();
        let mut cursor = tree.front_mut();
        while cursor.get().is_some_and(|node| node.off == *contiguous_off) {
            let segment = cursor.remove().unwrap();
            *contiguous_off = segment.end;
            ready.extend(segment.bufs);
        }
        self.first_off = cursor.get().map(|node| node.off);
        self.restore_tree(tree);
        if self.first_off.is_some() {
            return;
        }
        if self
            .tail
            .as_ref()
            .is_some_and(|tail| tail.off == *contiguous_off)
        {
            let tail = self.tail.take().unwrap();
            *contiguous_off = tail.end;
            ready.extend(tail.bufs);
        }
        self.first_off = self.tail.as_ref().map(|tail| tail.off);
    }

    #[cfg(test)]
    pub(super) fn iter(&mut self) -> impl Iterator<Item = &Segment> {
        let mut cursor = self.tree.get_mut().unwrap().as_cursor();
        // Move from the cached maximum to null, then to the minimum.
        cursor.move_next();
        cursor.move_next();
        std::iter::from_fn(move || {
            let segment = cursor.get()?;
            cursor.move_next();
            Some(segment)
        })
        .chain(self.tail.as_deref())
    }

    #[cfg(test)]
    pub(super) fn len(&mut self) -> usize {
        self.iter().count()
    }

    #[cfg(test)]
    pub(super) fn validate_cache(&mut self) {
        let cursor = self.tree.get_mut().unwrap().as_cursor();
        // This also rejects a null cache when the linked tree is nonempty.
        assert!(cursor.peek_next().is_null());
        if let Some(tail) = self.tail.as_ref() {
            assert!(!tail.link.is_linked());
            if let Some(last) = cursor.get() {
                assert!(last.link.is_linked());
                assert!(last.off < tail.off);
                assert!(last.end <= tail.off);
            }
        } else {
            assert!(cursor.is_null());
        }
    }
}
