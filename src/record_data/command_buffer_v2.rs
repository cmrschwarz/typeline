use std::collections::VecDeque;

use super::{
    field_action::{FieldAction, FieldActionKind},
    field_data::RunLength,
};

struct ActionGroup {
    start: usize,
    count: usize,
    higher_pow2_next_action_group_idx: usize,
}

struct ActionGroupQueue {
    action_group_offset: usize,
    action_groups: VecDeque<ActionGroup>,
    action_offset: usize,
    actions: VecDeque<FieldAction>,
}

struct Actor {
    action_group_queues: Vec<ActionGroupQueue>,
}

pub type ActorIndex = usize;

pub struct ActionBuffer {
    actors: Vec<Actor>,
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
    type Item = (usize, u8);

    fn next(&mut self) -> Option<(usize, u8)> {
        if self.value >= self.end {
            return None;
        }
        if self.value == 0 {
            self.value = self.end;
            return Some((0, self.end.next_power_of_two().ilog2() as u8));
        }
        let val = self.value;
        let trailing_zeroes = self.value.trailing_zeros() as u8;
        self.value += 1 << trailing_zeroes;
        if self.value >= self.end {
            let bits = (self.end - val).next_power_of_two().ilog2();
            return Some((val, bits as u8));
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
    type Item = (usize, u8);

    fn next(&mut self) -> Option<(usize, u8)> {
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
        Some((val, bit))
    }
}

impl ActionBuffer {
    pub fn begin_action_group(&mut self, ai: ActorIndex) {
        assert!(self.pending_action_group_actor_idx.is_none());
        self.pending_action_group_actor_idx = Some(ai)
    }
    pub fn end_action_group(&mut self) {
        let ai = self.pending_action_group_actor_idx.take().unwrap();
        let action_count = self.pending_action_group_action_count;
        if action_count == 0 {
            return;
        }
        self.pending_action_group_action_count = 0;
        let agq = &mut self.actors[ai].action_group_queues[0];
        let actions_start = agq.actions.len() - action_count;
        #[cfg(feature = "debug_logging")]
        {
            println!(
                "ai {}: added ag {}:",
                ai,
                agq.action_group_offset + agq.action_groups.len()
            );
            for a in agq.actions.range(actions_start..) {
                println!("   > {:?}:", a);
            }
        }
        agq.action_groups.push_back(ActionGroup {
            start: agq.action_offset + actions_start,
            count: action_count,
            higher_pow2_next_action_group_idx: 0,
        });
        todo!("pow2 merging")
    }
    pub fn push_action(
        &mut self,
        kind: FieldActionKind,
        field_idx: usize,
        mut run_length: usize,
    ) {
        let ai = self.pending_action_group_actor_idx.unwrap();
        let actions = &mut self.actors[ai].action_group_queues[0].actions;
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
}

#[cfg(test)]
mod test {
    use crate::record_data::command_buffer_v2::{
        Pow2InsertStepsIter, Pow2LookupStepsIter,
    };

    fn collect_lookup_steps(start: usize, end: usize) -> Vec<(usize, u8)> {
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
    ) -> Vec<(usize, u8)> {
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
