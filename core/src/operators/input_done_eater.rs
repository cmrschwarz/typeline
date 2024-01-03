use super::transform::{TransformData, TransformId, TransformState};
use crate::{
    chain::ChainId,
    job_session::{add_transform_to_job, JobData, JobSession},
    record_data::{field::VOID_FIELD_ID, match_set::MatchSetId},
};

// we use this e.g. as the successor of TfForcat subchains, to prevent
// the continuation chain to receive 'input_done' the moment that
// the first subchain is completed
pub struct TfInputDoneEater {
    input_dones_to_eat: usize,
    handled_predecessor: Option<TransformId>,
}

pub fn setup_tf_input_done_eater(
    tf_state: &mut TransformState,
    input_dones_to_eat: usize,
) -> TransformData<'static> {
    tf_state.is_transparent = true;
    TransformData::InputDoneEater(TfInputDoneEater {
        input_dones_to_eat,
        handled_predecessor: None,
    })
}

pub fn handle_tf_input_done_eater(
    sess: &mut JobData,
    tf_id: TransformId,
    ide: &mut TfInputDoneEater,
) {
    let (batch_size, mut ps) = sess.tf_mgr.claim_all(tf_id);
    let tf = &mut sess.tf_mgr.transforms[tf_id];
    if ps.input_done {
        //TODO: we might have an ABA problem here
        let is_repeat = ide.handled_predecessor.is_some()
            && tf.predecessor == ide.handled_predecessor;
        if ide.input_dones_to_eat > 0 || is_repeat {
            ps.input_done = false;
            tf.input_is_done = false;
        }
        if ide.input_dones_to_eat > 0 && !is_repeat {
            ide.input_dones_to_eat -= 1;
            ide.handled_predecessor = tf.predecessor;
        }
    }
    sess.tf_mgr.submit_batch(tf_id, batch_size, ps.input_done);
}

pub fn add_input_done_eater(
    sess: &mut JobSession,
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
        None,
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
