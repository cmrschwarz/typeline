use crate::{
    context::{ContextData, Job, Session, Venture},
    field_data::record_set::RecordSet,
    job_session::{JobData, JobSession},
    operators::operator::OperatorId,
    scr_error::ScrError,
};
use std::sync::Arc;

pub(crate) struct WorkerThread<'a> {
    pub ctx_data: Arc<ContextData<'a>>,
}

impl<'a> WorkerThread<'a> {
    pub(crate) fn new(ctx_data: Arc<ContextData<'a>>) -> Self {
        Self { ctx_data }
    }
    pub fn run_venture(
        &mut self,
        _sess: &'a Session,
        _start_op_id: OperatorId,
        _input_data: Option<Arc<RecordSet>>,
    ) {
    }
    pub fn run_job(&mut self, sess: &'a Session, job: Job) {
        let mut js = JobSession {
            transform_data: Default::default(),
            job_data: JobData::new(sess),
            temp_vec: Default::default(),
        };
        match js.run_job(job, Some(&self.ctx_data)) {
            Ok(_) => (),
            Err(venture_desc) => {
                let mut sess_mgr = self.ctx_data.sess_mgr.lock().unwrap();
                sess_mgr.venture_queue.push_back(Venture {
                    description: venture_desc,
                    input_data: Some(Arc::new(js.into_record_set())),
                });
            }
        }
    }

    pub fn run(&mut self) -> Result<(), ScrError> {
        let mut sess_mgr = self.ctx_data.sess_mgr.lock().unwrap();
        loop {
            if !sess_mgr.terminate {
                if let Some(venture) = sess_mgr.venture_queue.front() {
                    let sess = sess_mgr.session;
                    let input_data = venture.input_data.clone();
                    let start_op_id =
                        venture.description.starting_points[sess_mgr.waiting_venture_participants];
                    let participants_needed = venture.description.participans_needed;
                    sess_mgr.waiting_venture_participants += 1;
                    if sess_mgr.waiting_venture_participants == participants_needed {
                        sess_mgr.venture_queue.pop_front();
                        sess_mgr.venture_counter = sess_mgr.venture_counter.wrapping_add(1);
                        drop(sess_mgr);
                        self.ctx_data.work_available.notify_all();
                    } else {
                        let counter = sess_mgr.venture_counter;
                        loop {
                            sess_mgr = self.ctx_data.work_available.wait(sess_mgr).unwrap();
                            if counter != sess_mgr.venture_counter {
                                drop(sess_mgr);
                                break;
                            }
                        }
                    }
                    self.run_venture(sess, start_op_id, input_data);
                    sess_mgr = self.ctx_data.sess_mgr.lock().unwrap();
                    continue;
                }
                if let Some(job) = sess_mgr.job_queue.pop_front() {
                    let sess = sess_mgr.session;
                    drop(sess_mgr);
                    self.run_job(sess, job);
                    sess_mgr = self.ctx_data.sess_mgr.lock().unwrap();
                    continue;
                }
            }
            sess_mgr.waiting_worker_threads += 1;
            if sess_mgr.waiting_worker_threads == sess_mgr.total_worker_threads {
                self.ctx_data.worker_threads_finished.notify_one();
            }
            if sess_mgr.terminate {
                return Ok(());
            }
            sess_mgr = self.ctx_data.work_available.wait(sess_mgr).unwrap();
        }
    }
}
