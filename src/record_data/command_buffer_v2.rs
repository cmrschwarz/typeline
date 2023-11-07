use std::collections::VecDeque;

use super::{
    field_action::{FieldAction, FieldActionKind},
    field_data::RunLength,
};

struct ActionGroup {
    start: usize,
    count: usize,
    refcount: usize,
    next_action_idx: usize,
}

struct ActionGroupMetadata {
    refcount: usize,
    next_action_idx: usize,
}

#[derive(Default)]
struct ActionGroupQueue {
    action_groups_offset: usize,
    action_groups: VecDeque<ActionGroup>,
    actions_offset: usize,
    actions: VecDeque<FieldAction>,
    dirty: bool,
    refcount: usize,
}

struct Actor {
    action_group_queues: Vec<ActionGroupQueue>,
    refcount: usize,
}

struct ActionGroupIdentifier {
    actor_idx: usize,
    pow2: usize,
    start: usize,
    count: usize,
}

pub struct ActionBuffer {
    actors_offset: usize,
    actors: VecDeque<Actor>,
    action_temp_buffers: [VecDeque<FieldAction>; 3],
    pending_action_group_actor_idx: Option<usize>,
    pending_action_group_action_count: usize,
}

struct Pow2LookupStepsIter {
    value: usize,
    end: usize,
}

struct Pow2InsertStepsIter {
    value: usize,
    begin: usize,
    end: usize,
    bit: u8,
    bit_count: u8,
}

impl Pow2LookupStepsIter {
    pub fn new(start: usize, end: usize) -> Self {
        Self { value: start, end }
    }
}

impl Iterator for Pow2LookupStepsIter {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<(usize, usize)> {
        if self.value >= self.end {
            return None;
        }
        if self.value == 0 {
            self.value = self.end;
            return Some((0, self.end.next_power_of_two().ilog2() as usize));
        }
        let val = self.value;
        let trailing_zeroes = self.value.trailing_zeros() as usize;
        self.value += 1 << trailing_zeroes;
        if self.value >= self.end {
            let bits = (self.end - val).next_power_of_two().ilog2();
            return Some((val, bits as usize));
        }
        Some((val, trailing_zeroes))
    }
}

impl Pow2InsertStepsIter {
    pub fn new(index: usize, begin: usize, end: usize) -> Self {
        Self {
            value: index,
            begin,
            end,
            bit: 0,
            bit_count: end.next_power_of_two().ilog2() as u8 + 1,
        }
    }
}

impl Iterator for Pow2InsertStepsIter {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<(usize, usize)> {
        if self.value < self.begin || self.bit == self.bit_count {
            return None;
        }
        let val = self.value;
        let bit = self.bit;
        self.bit += 1;
        let shift = 1 << bit;
        if self.value >= shift {
            self.value -= self.value & shift;
        } else {
            self.value = 0;
        }
        if val + (shift >> 1) >= self.end {
            return self.next();
        }
        Some((val, bit as usize))
    }
}

impl ActionBuffer {
    pub fn begin_action_group(&mut self, actor_idx: usize) {
        assert!(self.pending_action_group_actor_idx.is_none());
        self.pending_action_group_actor_idx = Some(actor_idx)
    }
    pub fn end_action_group(&mut self) {
        let ai = self.pending_action_group_actor_idx.take().unwrap();
        let action_count = self.pending_action_group_action_count;
        if action_count == 0 {
            return;
        }
        self.pending_action_group_action_count = 0;
        let agq =
            &mut self.actors[ai - self.actors_offset].action_group_queues[0];
        let actions_start = agq.actions.len() - action_count;
        #[cfg(feature = "debug_logging")]
        {
            println!(
                "ai {}: added ag {}:",
                ai,
                agq.action_groups_offset + agq.action_groups.len()
            );
            for a in agq.actions.range(actions_start..) {
                println!("   > {:?}:", a);
            }
        }
        agq.action_groups.push_back(ActionGroup {
            start: agq.actions_offset + actions_start,
            count: action_count,
            refcount: 0,
            next_action_idx: 0,
        });
        for (i, pow2) in
            Pow2InsertStepsIter::new(ai, self.actors_offset, self.actors.len())
        {
            let actor = &mut self.actors[i - self.actors_offset];
            let agq = &mut actor.action_group_queues[pow2];
            if agq.dirty {
                break;
            }
            agq.dirty = true;
        }
    }
    pub fn push_action(
        &mut self,
        kind: FieldActionKind,
        field_idx: usize,
        mut run_length: usize,
    ) {
        let actor_idx = self.pending_action_group_actor_idx.unwrap();
        let actions = &mut self.actors[actor_idx - self.actors_offset]
            .action_group_queues[0]
            .actions;
        if self.pending_action_group_action_count > 0 {
            let last = actions.back_mut().unwrap();
            // field indices in action groups must be ascending
            debug_assert!(last.field_idx <= field_idx);
            // very simple early merging of actions to hopefully save some
            // memory this also allows operations to be slightly
            // more 'wasteful' with their action pushes
            if last.kind == kind && last.field_idx == field_idx {
                let run_len_rem = (RunLength::MAX - last.run_len) as usize;
                if run_len_rem >= run_length {
                    last.run_len += run_length as RunLength;
                    return;
                }
                last.run_len = RunLength::MAX;
                run_length -= run_len_rem;
            }
        }
        while run_length > 0 {
            let rl_to_push =
                run_length.min(RunLength::MAX as usize) as RunLength;
            actions.push_back(FieldAction {
                kind,
                field_idx,
                run_len: rl_to_push,
            });
            self.pending_action_group_action_count += 1;
            run_length -= rl_to_push as usize;
        }
    }
    pub fn add_actor(&mut self) -> usize {
        let actor_idx = self.actors.len() + self.actors_offset;
        let mut ac = Actor {
            action_group_queues: Vec::new(),
            refcount: 1,
        };
        ac.action_group_queues.push(ActionGroupQueue::default());
        self.actors.push_back(ac);
        if actor_idx != 0 {
            let pow2_to_add = actor_idx.trailing_zeros() + 1;
            let tgt_actor_id = actor_idx - (1 << pow2_to_add);
            let tgt_actor =
                &mut self.actors[tgt_actor_id - self.actors_offset];
            debug_assert_eq!(
                tgt_actor.action_group_queues.len(),
                pow2_to_add as usize
            );
            tgt_actor.action_group_queues.push(ActionGroupQueue {
                dirty: true,
                ..Default::default()
            })
        }
        actor_idx
    }
    fn refresh_action_group(&mut self, actor_idx: usize, pow2: usize) {}
    fn build_action_list(
        &mut self,
        actor_idx: usize,
        last_observed_group: usize,
        target: &mut Vec<FieldAction>,
    ) -> Option<ActionGroupIdentifier> {
        for (ai, pow2) in
            Pow2LookupStepsIter::new(actor_idx, self.actors.len())
        {}
        None
    }
}

#[cfg(test)]
mod test {
    use crate::record_data::command_buffer_v2::{
        Pow2InsertStepsIter, Pow2LookupStepsIter,
    };

    fn collect_lookup_steps(start: usize, end: usize) -> Vec<(usize, usize)> {
        Pow2LookupStepsIter::new(start, end).collect::<Vec<_>>()
    }

    #[test]
    fn test_pow2_lookup_steps_iter() {
        assert_eq!(collect_lookup_steps(0, 0), []);
        assert_eq!(collect_lookup_steps(0, 1), [(0, 0)]);
        assert_eq!(collect_lookup_steps(0, 2), [(0, 1)]);
        assert_eq!(collect_lookup_steps(0, 3), [(0, 2)]);
        assert_eq!(collect_lookup_steps(0, 4), [(0, 2)]);
        assert_eq!(collect_lookup_steps(0, 5), [(0, 3)]);
        assert_eq!(collect_lookup_steps(1, 0), []);
        assert_eq!(collect_lookup_steps(1, 1), []);
        assert_eq!(collect_lookup_steps(1, 2), [(1, 0)]);
        assert_eq!(collect_lookup_steps(1, 3), [(1, 0), (2, 0)]);
        assert_eq!(collect_lookup_steps(1, 4), [(1, 0), (2, 1)]);
        assert_eq!(collect_lookup_steps(2, 8), [(2, 1), (4, 2)]);
    }

    fn collect_insert_steps(
        index: usize,
        begin: usize,
        end: usize,
    ) -> Vec<(usize, usize)> {
        Pow2InsertStepsIter::new(index, begin, end).collect::<Vec<_>>()
    }

    #[test]
    fn test_pow2_insert_steps_iter() {
        assert_eq!(collect_insert_steps(0, 0, 0), []);
        assert_eq!(collect_insert_steps(1, 0, 0), []);
        assert_eq!(collect_insert_steps(1, 0, 1), []);
        assert_eq!(collect_insert_steps(0, 0, 1), [(0, 0)]);
        assert_eq!(collect_insert_steps(0, 0, 3), [(0, 0), (0, 1), (0, 2)]);
        assert_eq!(collect_insert_steps(1, 0, 2), [(1, 0), (0, 1)]);
        assert_eq!(collect_insert_steps(2, 0, 3), [(2, 0), (0, 2)]);
        assert_eq!(
            collect_insert_steps(7, 0, 8),
            [(7, 0), (6, 1), (4, 2), (0, 3)]
        );
        assert_eq!(collect_insert_steps(8, 0, 9), [(8, 0), (0, 4)]);
        assert_eq!(
            collect_insert_steps(1, 0, 9),
            [(1, 0), (0, 1), (0, 2), (0, 3), (0, 4)],
        );
    }
}
