use super::transform::{TransformData, TransformId, TransformState};
use crate::{
    chain::ChainId,
    job::{add_transform_to_job, Job, JobData},
    record_data::{
        field::VOID_FIELD_ID, match_set::MatchSetId,
        record_group_tracker::VOID_GROUP_LIST_ID,
    },
};

// we use this e.g. as the successor of TfForcat subchains, to prevent
// the continuation chain to receive 'input_done' the moment that
// the first subchain is completed
pub struct TfInputDoneEater {
    input_dones_to_eat: usize,
}

pub fn setup_tf_input_done_eater<'a>(
    tf_state: &mut TransformState,
    input_dones_to_eat: usize,
) -> TransformData<'a> {
    tf_state.is_transparent = true;
    TransformData::InputDoneEater(TfInputDoneEater { input_dones_to_eat })
}

pub fn handle_tf_input_done_eater(
    jd: &mut JobData,
    tf_id: TransformId,
    ide: &mut TfInputDoneEater,
) {
    let (batch_size, mut ps) = jd.tf_mgr.claim_all(tf_id);
    let tf = &mut jd.tf_mgr.transforms[tf_id];
    if ps.input_done && ide.input_dones_to_eat > 0 {
        ps.input_done = false;
        tf.predecessor_done = false;
        ide.input_dones_to_eat -= 1;
        #[cfg(feature = "debug_logging")]
        println!(
            "  tf {:02}: input_done_eater chomped, {} remaining",
            tf_id, ide.input_dones_to_eat
        );
    }
    jd.match_set_mgr.update_cow_targets(
        &jd.field_mgr,
        jd.tf_mgr.transforms[tf_id].match_set_id,
    );
    jd.tf_mgr.submit_batch(tf_id, batch_size, ps.input_done);
}

pub fn add_input_done_eater<'a>(
    sess: &mut Job<'a>,
    chain_id: ChainId, // to get desired batch size
    ms_id: MatchSetId,
    input_dones_to_eat: usize,
) -> TransformId {
    let batch_size = sess.job_data.session_data.chains[chain_id as usize]
        .settings
        .default_batch_size;
    let mut tf_state = TransformState::new(
        VOID_FIELD_ID,
        VOID_FIELD_ID,
        ms_id,
        batch_size,
        None,
        // TODO: remove this guy completely in favor of foreach trailer
        VOID_GROUP_LIST_ID,
    );
    sess.job_data.field_mgr.inc_field_refcount(VOID_FIELD_ID, 2);
    let tf_data = setup_tf_input_done_eater(&mut tf_state, input_dones_to_eat);
    add_transform_to_job(
        &mut sess.job_data,
        &mut sess.transform_data,
        tf_state,
        tf_data,
    )
}
