#[derive(Copy, Clone, Debug)]
pub struct Statistics {
    pub mutator_id: u32,
    pub request_id: u32,
    pub write_barrier_counter: u32,
    pub write_barrier_slowpath_counter: u32,
    pub write_barrier_publish_counter: u32,
    pub write_barrier_publish_bytes: usize,
    pub request_scope_bytes_published: usize,
    pub request_scope_live_public_object_size: usize,
    pub request_scope_live_public_object_counter: u32,
    pub request_scope_live_private_object_size: usize,
    pub request_scope_live_private_object_counter: u32,
}

impl Statistics {
    pub fn new(mutator_id: u32, request_id: u32) -> Self {
        Statistics {
            mutator_id,
            request_id,
            write_barrier_counter: 0,
            write_barrier_slowpath_counter: 0,
            write_barrier_publish_counter: 0,
            write_barrier_publish_bytes: 0,
            request_scope_bytes_published: 0,
            request_scope_live_public_object_size: 0,
            request_scope_live_public_object_counter: 0,
            request_scope_live_private_object_size: 0,
            request_scope_live_private_object_counter: 0,
        }
    }

    pub fn reset(&mut self, mutator_id: u32, request_id: u32) {
        self.mutator_id = mutator_id;
        self.request_id = request_id;
        self.write_barrier_counter = 0;
        self.write_barrier_slowpath_counter = 0;
        self.write_barrier_publish_counter = 0;
        self.write_barrier_publish_bytes = 0;
        self.request_scope_bytes_published = 0;
        self.request_scope_live_public_object_size = 0;
        self.request_scope_live_public_object_counter = 0;
        self.request_scope_live_private_object_size = 0;
        self.request_scope_live_private_object_counter = 0;
    }

    pub fn merge(&mut self, s: Statistics) {
        // assert!(
        //     self.mutator_id == s.mutator_id && (self.request_id == s.request_id,
        //     "cannot merge irrelevant statistics. {}.{} vs {}.{}",
        //     self.mutator_id,
        //     self.request_id,
        //     s.mutator_id,
        //     s.request_id
        // );

        if self.write_barrier_counter == 0 {
            self.write_barrier_counter = s.write_barrier_counter;
        }
        if self.write_barrier_slowpath_counter == 0 {
            self.write_barrier_slowpath_counter = s.write_barrier_slowpath_counter;
        }
        if self.write_barrier_publish_counter == 0 {
            self.write_barrier_publish_counter = s.write_barrier_publish_counter;
        }
        if self.write_barrier_publish_bytes == 0 {
            self.write_barrier_publish_bytes = s.write_barrier_publish_bytes;
        }
        if self.request_scope_bytes_published == 0 {
            self.request_scope_bytes_published = s.request_scope_bytes_published;
        }
        if self.request_scope_live_public_object_size == 0 {
            self.request_scope_live_public_object_size = s.request_scope_live_public_object_size;
        }
        if self.request_scope_live_public_object_counter == 0 {
            self.request_scope_live_public_object_counter =
                s.request_scope_live_public_object_counter;
        }
        if self.request_scope_live_private_object_size == 0 {
            self.request_scope_live_private_object_size = s.request_scope_live_private_object_size;
        }
        if self.request_scope_live_private_object_counter == 0 {
            self.request_scope_live_private_object_counter =
                s.request_scope_live_private_object_counter;
        }
    }
}