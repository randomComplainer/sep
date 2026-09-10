#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Id {
    pub slot: u8,
    pub ver: u8,
}

impl Id {
    pub fn new(slot: u8, ver: u8) -> Self {
        Self { slot, ver }
    }
}

struct Entry {
    ver: u8,
    // u8::MAX is reserved for tagging occupied slot
    next: u8,
}

pub struct IdSpace {
    entries: Box<[Entry]>,
    // u8::MAX when all items are occupied
    head: u8,
    // u8::MAX when all items are occupied
    tail: u8,
    max_ver: u8,
}

impl IdSpace {
    pub fn new(capacity: u8, max_ver: u8) -> Self {
        if capacity == u8::MAX {
            panic!("capacity too large");
        }

        let mut entries = Vec::with_capacity(capacity as usize);

        for i in 0..capacity {
            entries.push(Entry {
                ver: 0,
                next: if i == capacity - 1 { 0 } else { i + 1 },
            });
        }

        Self {
            head: 0,
            tail: capacity - 1,
            max_ver,
            entries: entries.into_boxed_slice(),
        }
    }

    pub fn allocate(&mut self) -> Option<Id> {
        if self.head == u8::MAX {
            return None;
        }

        assert_ne!(self.tail, u8::MAX);

        let entry = unsafe { self.entries.get_unchecked_mut(self.head as usize) };

        let id = Id::new(self.head, entry.ver);

        // it's the last entry
        if entry.next == self.head {
            self.head = u8::MAX;
            self.tail = u8::MAX;
            entry.next = u8::MAX;
        } else {
            self.head = entry.next;
            entry.next = u8::MAX;
            unsafe {
                self.entries.get_unchecked_mut(self.tail as usize).next = self.head;
            }
        }

        Some(id)
    }

    pub fn free(&mut self, id: Id) {
        assert!(id.ver <= self.max_ver);
        assert!((id.slot as usize) < self.entries.len());

        if id.ver == self.max_ver {
            return;
        }

        let entry = unsafe { self.entries.get_unchecked_mut(id.slot as usize) };

        if id.ver < entry.ver {
            return;
        }

        entry.ver += 1;
        entry.next = self.head;

        // was empty
        if self.head == u8::MAX {
            assert_eq!(self.tail, u8::MAX);

            self.head = id.slot;
            self.tail = id.slot;
            entry.next = id.slot;
        } else {
            assert_ne!(self.tail, u8::MAX);

            unsafe {
                self.entries.get_unchecked_mut(self.tail as usize).next = id.slot;
            }
            self.tail = id.slot;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn happy_path() {
        let mut space = IdSpace::new(3, 1);

        assert_eq!(Some(Id::new(0, 0)), space.allocate());
        assert_eq!(Some(Id::new(1, 0)), space.allocate());
        assert_eq!(Some(Id::new(2, 0)), space.allocate());
        assert_eq!(None, space.allocate());
        space.free(Id::new(2, 0));
        space.free(Id::new(1, 0));
        space.free(Id::new(0, 0));
        assert_eq!(Some(Id::new(2, 1)), space.allocate());
        assert_eq!(Some(Id::new(1, 1)), space.allocate());
        assert_eq!(Some(Id::new(0, 1)), space.allocate());
        assert_eq!(None, space.allocate());
        space.free(Id::new(1, 0));
        assert_eq!(None, space.allocate());
    }

    #[test]
    fn fifo() {
        let mut space = IdSpace::new(3, 1);

        assert_eq!(Some(Id::new(0, 0)), space.allocate());
        space.free(Id::new(0, 0));

        assert_eq!(Some(Id::new(1, 0)), space.allocate());

        assert_eq!(Some(Id::new(2, 0)), space.allocate());
        space.free(Id::new(2, 0));
        space.free(Id::new(1, 0));

        assert_eq!(Some(Id::new(0, 1)), space.allocate());
        assert_eq!(Some(Id::new(2, 1)), space.allocate());
        assert_eq!(Some(Id::new(1, 1)), space.allocate());
        assert_eq!(None, space.allocate());
    }


    #[test]
    fn exhausted_slot() {
        let mut space = IdSpace::new(3, 1);
        assert_eq!(Some(Id::new(0, 0)), space.allocate());
        assert_eq!(Some(Id::new(1, 0)), space.allocate());
        assert_eq!(Some(Id::new(2, 0)), space.allocate());
        assert_eq!(None, space.allocate());

        space.free(Id::new(1, 0));
        assert_eq!(Some(Id::new(1, 1)), space.allocate());
        space.free(Id::new(1, 1));
        assert_eq!(None, space.allocate());

        space.free(Id::new(2, 0));
        assert_eq!(Some(Id::new(2, 1)), space.allocate());
        space.free(Id::new(2, 1));
        assert_eq!(None, space.allocate());

        space.free(Id::new(0, 0));
        assert_eq!(Some(Id::new(0, 1)), space.allocate());
        space.free(Id::new(0, 1));
        assert_eq!(None, space.allocate());
    }

    #[test]
    fn free_old_version_has_no_effect() {
        let mut space = IdSpace::new(1, 3);
        assert_eq!(Some(Id::new(0, 0)), space.allocate());

        space.free(Id::new(0, 0));
        assert_eq!(Some(Id::new(0, 1)), space.allocate());

        space.free(Id::new(0, 0));
        assert_eq!(None, space.allocate());

        space.free(Id::new(0, 1));
        assert_eq!(Some(Id::new(0, 2)), space.allocate());
    }
}
