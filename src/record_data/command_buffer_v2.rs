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
