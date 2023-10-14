use super::field_data::RunLength;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FieldActionKind {
    #[default]
    Dup,
    Drop,
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct FieldAction {
    pub kind: FieldActionKind,
    pub field_idx: usize,
    pub run_len: RunLength,
}

impl FieldAction {
    pub fn new(
        kind: FieldActionKind,
        field_idx: usize,
        run_len: RunLength,
    ) -> Self {
        Self {
            kind,
            field_idx,
            run_len,
        }
    }
}

fn push_merged_action(
    target: &mut Vec<FieldAction>,
    first_insert: &mut bool,
    kind: FieldActionKind,
    field_idx: usize,
    mut run_len: usize,
) {
    if *first_insert {
        if run_len == 0 {
            return;
        }
        *first_insert = false;
    } else {
        let prev = target.last_mut().unwrap();
        if prev.field_idx == field_idx && prev.kind == kind {
            let space_rem =
                (RunLength::MAX as usize - prev.run_len as usize).min(run_len);
            prev.run_len += space_rem as RunLength;
            run_len -= space_rem;
        }
    }
    let mut action = FieldAction {
        kind,
        field_idx,
        run_len: 0,
    };
    while run_len > RunLength::MAX as usize {
        action.run_len = RunLength::MAX;
        target.push(action);
        run_len -= RunLength::MAX as usize;
    }
    if run_len > 0 {
        action.run_len = run_len as RunLength;
        target.push(action);
    }
}

pub fn merge_action_lists(
    sets: [&[FieldAction]; 2],
    target: &mut Vec<FieldAction>,
) {
    let left = sets[0];
    let right = sets[1];
    let mut first_insert = true;

    let (mut curr_action_idx_left, mut curr_action_idx_right) = (0, 0);
    let mut next_action_field_idx_left;
    let mut next_action_field_idx_right;
    let mut field_pos_offset_left = 0isize;
    let mut outstanding_drops_right = 0usize;
    loop {
        if curr_action_idx_left == left.len() {
            break;
        }
        if curr_action_idx_right < right.len() {
            next_action_field_idx_right = right[curr_action_idx_right]
                .field_idx
                + outstanding_drops_right;
        } else {
            next_action_field_idx_right = usize::MAX;
        }
        next_action_field_idx_left =
            (left[curr_action_idx_left].field_idx as isize
                + field_pos_offset_left) as usize;
        if next_action_field_idx_left <= next_action_field_idx_right {
            let action_left = &left[curr_action_idx_left];
            let field_idx = (action_left.field_idx as isize
                + field_pos_offset_left) as usize;
            let mut run_len = action_left.run_len as usize;
            let mut kind = action_left.kind;

            match action_left.kind {
                FieldActionKind::Dup => {
                    let space_to_next = left
                        .get(curr_action_idx_left + 1)
                        .map(|a| a.field_idx - action_left.field_idx)
                        .unwrap_or(usize::MAX);
                    if outstanding_drops_right >= run_len {
                        kind = FieldActionKind::Drop;
                        outstanding_drops_right -= run_len;
                        run_len = outstanding_drops_right.min(space_to_next);
                        outstanding_drops_right -= run_len;
                        field_pos_offset_left -= run_len as isize;
                    } else {
                        run_len -= outstanding_drops_right;
                        outstanding_drops_right = 0;
                    }
                }
                FieldActionKind::Drop => {
                    outstanding_drops_right += run_len;
                    run_len = outstanding_drops_right;
                    outstanding_drops_right -= run_len;
                }
            }
            push_merged_action(
                target,
                &mut first_insert,
                kind,
                field_idx,
                run_len,
            );
            curr_action_idx_left += 1;
        } else {
            debug_assert!(outstanding_drops_right == 0);
            let right = &right[curr_action_idx_right];
            let field_idx = right.field_idx;
            let mut run_len = right.run_len as usize;

            match right.kind {
                FieldActionKind::Dup => {
                    field_pos_offset_left += run_len as isize;
                }
                FieldActionKind::Drop => {
                    let gap_to_start_left = next_action_field_idx_left
                        - next_action_field_idx_right;
                    if gap_to_start_left < run_len {
                        outstanding_drops_right += run_len - gap_to_start_left;
                        run_len = gap_to_start_left;
                    }
                    field_pos_offset_left -= run_len as isize;
                }
            }
            push_merged_action(
                target,
                &mut first_insert,
                right.kind,
                field_idx,
                run_len,
            );
            curr_action_idx_right += 1;
        }
    }
    for action in &right[curr_action_idx_right..] {
        push_merged_action(
            target,
            &mut first_insert,
            action.kind,
            action.field_idx,
            action.run_len as usize,
        );
    }
}

#[cfg(test)]
mod test {
    use super::FieldActionKind;

    use super::FieldAction;

    fn compare_merge_result(
        left: &[FieldAction],
        right: &[FieldAction],
        out: &[FieldAction],
    ) {
        let mut output = Vec::new();
        super::merge_action_lists([left, right], &mut output);
        assert_eq!(output.as_slice(), out);
    }
    #[test]
    fn uncontested_drops_survive_merge() {
        use FieldActionKind::*;
        let drops = &[
            FieldAction::new(Drop, 0, 2),
            FieldAction::new(Drop, 1, 9),
            FieldAction::new(Drop, 2, 7),
        ];
        compare_merge_result(drops, &[], drops);
        compare_merge_result(&[], drops, drops);
    }

    #[test]
    fn actions_are_merged() {
        for kind in [FieldActionKind::Dup, FieldActionKind::Drop] {
            let unmerged = &[
                FieldAction {
                    kind,
                    field_idx: 0,
                    run_len: 1,
                },
                FieldAction {
                    kind,
                    field_idx: 0,
                    run_len: 1,
                },
            ];
            let blank = &[];
            let merged = &[FieldAction {
                kind,
                field_idx: 0,
                run_len: 2,
            }];
            compare_merge_result(unmerged, blank, merged);
            compare_merge_result(blank, unmerged, merged);
        }
    }
    #[test]
    fn left_field_indices_are_adjusted() {
        let left = &[FieldAction {
            kind: FieldActionKind::Drop,
            field_idx: 1,
            run_len: 1,
        }];
        let right = &[FieldAction {
            kind: FieldActionKind::Dup,
            field_idx: 0,
            run_len: 5,
        }];
        let merged = &[
            FieldAction {
                kind: FieldActionKind::Dup,
                field_idx: 0,
                run_len: 5,
            },
            FieldAction {
                kind: FieldActionKind::Drop,
                field_idx: 6,
                run_len: 1,
            },
        ];
        compare_merge_result(left, right, merged);
    }

    #[test]
    fn encompassed_dups_are_deleted() {
        let left = &[FieldAction {
            kind: FieldActionKind::Dup,
            field_idx: 1,
            run_len: 1,
        }];
        let right = &[FieldAction {
            kind: FieldActionKind::Drop,
            field_idx: 0,
            run_len: 5,
        }];
        let merged = &[FieldAction {
            kind: FieldActionKind::Drop,
            field_idx: 0,
            run_len: 4,
        }];
        compare_merge_result(left, right, merged);
    }

    #[test]
    fn interrupted_left_actions() {
        let left = &[
            FieldAction {
                kind: FieldActionKind::Dup,
                field_idx: 1,
                run_len: 1,
            },
            FieldAction {
                kind: FieldActionKind::Dup,
                field_idx: 10,
                run_len: 1,
            },
        ];
        let right = &[
            FieldAction {
                kind: FieldActionKind::Drop,
                field_idx: 0,
                run_len: 5,
            },
            FieldAction {
                kind: FieldActionKind::Drop,
                field_idx: 2,
                run_len: 3,
            },
        ];
        let merged = &[
            FieldAction {
                kind: FieldActionKind::Drop,
                field_idx: 0,
                run_len: 4,
            },
            FieldAction {
                kind: FieldActionKind::Drop,
                field_idx: 2,
                run_len: 3,
            },
            FieldAction {
                kind: FieldActionKind::Dup,
                field_idx: 3,
                run_len: 1,
            },
        ];
        compare_merge_result(left, right, merged);
    }

    #[test]
    fn chained_right_drops() {
        let left = &[FieldAction {
            kind: FieldActionKind::Dup,
            field_idx: 10,
            run_len: 1,
        }];
        let right = &[
            FieldAction {
                kind: FieldActionKind::Drop,
                field_idx: 0,
                run_len: 1,
            },
            FieldAction {
                kind: FieldActionKind::Drop,
                field_idx: 2,
                run_len: 1,
            },
        ];
        let merged = &[
            FieldAction {
                kind: FieldActionKind::Drop,
                field_idx: 0,
                run_len: 1,
            },
            FieldAction {
                kind: FieldActionKind::Drop,
                field_idx: 2,
                run_len: 1,
            },
            FieldAction {
                kind: FieldActionKind::Dup,
                field_idx: 8,
                run_len: 1,
            },
        ];
        compare_merge_result(left, right, merged);
    }

    #[test]
    fn overlapping_drops() {
        let a = &[FieldAction {
            kind: FieldActionKind::Drop,
            field_idx: 3,
            run_len: 5,
        }];
        let b = &[FieldAction {
            kind: FieldActionKind::Drop,
            field_idx: 2,
            run_len: 3,
        }];
        let merged_a_b = &[FieldAction {
            kind: FieldActionKind::Drop,
            field_idx: 2,
            run_len: 8,
        }];
        let merged_b_a = &[
            FieldAction {
                kind: FieldActionKind::Drop,
                field_idx: 2,
                run_len: 3,
            },
            FieldAction {
                kind: FieldActionKind::Drop,
                field_idx: 3,
                run_len: 5,
            },
        ];
        compare_merge_result(a, b, merged_a_b);
        compare_merge_result(b, a, merged_b_a);
    }
}
