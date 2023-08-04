use crate::{
    context::{ContextData, Job, Session},
    job_session::{JobData, JobSession},
    operators::operator::OperatorId,
    record_data::record_buffer::RecordBuffer,
};
use std::sync::Arc;

pub(crate) struct WorkerThread {
    pub ctx_data: Arc<ContextData>,
}

impl WorkerThread {
    pub(crate) fn new(ctx_data: Arc<ContextData>) -> Self {
        Self { ctx_data }
    }
    pub fn run_venture<'a>(
        &mut self,
        sess: &'a Session,
        start_op_id: OperatorId,
        buffer: Arc<RecordBuffer>,
        mut starting_job_session: Option<Box<JobSession<'a>>>,
    ) {
        if let Some(ref mut js) = starting_job_session {
            match js.run(Some(&self.ctx_data)) {
                Ok(_) => (),
                Err(venture_desc) => {
                    self.ctx_data.sess_mgr.lock().unwrap().submit_venture(
                        venture_desc,
                        starting_job_session,
                        &self.ctx_data,
                    );
                }
            }
        }
        let mut js = JobSession {
            transform_data: Default::default(),
            job_data: JobData::new(sess),
            temp_vec: Default::default(),
        };
        js.setup_venture(Some(&self.ctx_data), buffer, start_op_id);
        match js.run(Some(&self.ctx_data)) {
            Ok(_) => (),
            Err(venture_desc) => {
                self.ctx_data.sess_mgr.lock().unwrap().submit_venture(
                    venture_desc,
                    Some(Box::new(js)),
                    &self.ctx_data,
                );
            }
        }
    }

    pub fn run_job(&mut self, sess: &Session, job: Job) {
        let mut js = JobSession {
            transform_data: Default::default(),
            job_data: JobData::new(sess),
            temp_vec: Default::default(),
        };
        js.setup_job(job);
        match js.run(Some(&self.ctx_data)) {
            Ok(_) => (),
            Err(venture_desc) => {
                self.ctx_data.sess_mgr.lock().unwrap().submit_venture(
                    venture_desc,
                    Some(Box::new(js)),
                    &self.ctx_data,
                );
            }
        }
    }

    pub fn run(&mut self) {
        let mut sess_mgr = self.ctx_data.sess_mgr.lock().unwrap();
        loop {
            if !sess_mgr.terminate {
                let waiting_venture_participants =
                    sess_mgr.waiting_venture_participants;
                if let Some(venture) = sess_mgr.venture_queue.front_mut() {
                    let job_sess = venture.source_job_session.take();
                    let buffer = venture.description.buffer.clone();
                    let participants_needed =
                        venture.description.participans_needed;
                    let start_op_id = venture.description.starting_points
                        [waiting_venture_participants];
                    let sess;
                    sess_mgr.waiting_venture_participants += 1;
                    if sess_mgr.waiting_venture_participants
                        == participants_needed
                    {
                        sess_mgr.waiting_venture_participants = 0;
                        sess_mgr.venture_queue.pop_front();
                        sess_mgr.venture_id_counter =
                            sess_mgr.venture_id_counter.wrapping_add(1);
                        sess = sess_mgr.session.clone();
                        drop(sess_mgr);
                        self.ctx_data.work_available.notify_all();
                    } else {
                        let counter = sess_mgr.venture_id_counter;
                        loop {
                            if counter != sess_mgr.venture_id_counter {
                                sess = sess_mgr.session.clone();
                                drop(sess_mgr);
                                break;
                            }
                            sess_mgr = self
                                .ctx_data
                                .work_available
                                .wait(sess_mgr)
                                .unwrap();
                        }
                    }
                    self.run_venture(&sess, start_op_id, buffer, job_sess);
                    sess_mgr = self.ctx_data.sess_mgr.lock().unwrap();
                    continue;
                }
                if let Some(job) = sess_mgr.job_queue.pop_front() {
                    let sess = sess_mgr.session.clone();
                    drop(sess_mgr);
                    self.run_job(&sess, job);
                    sess_mgr = self.ctx_data.sess_mgr.lock().unwrap();
                    continue;
                }
            }
            sess_mgr.waiting_worker_threads += 1;
            if sess_mgr.waiting_worker_threads == sess_mgr.total_worker_threads
            {
                self.ctx_data.worker_threads_finished.notify_one();
            }
            if sess_mgr.terminate {
                return;
            }
            sess_mgr = self.ctx_data.work_available.wait(sess_mgr).unwrap();
        }
    }
}
