use crate::{
    context::{ContextData, Job, Session},
    operations::transform::TransformData,
    scr_error::ScrError,
    worker_thread_session::{JobSession, WorkerThreadSession},
};
use std::sync::Arc;

pub(crate) struct WorkerThread {
    pub ctx_data: Arc<ContextData>,
    pub session: Arc<Session>,
    // SAFETY: job_session and transform_data refer to session, but this type make sure that
    // their references get updated before the session (that this type holds) goes out of scope.
    // we do this to avoid reallocationg JobSession and Vec<TransformData> for every single job
    pub job_session: JobSession<'static>,
    pub transform_data: Vec<TransformData<'static>>,
}

impl WorkerThread {
    pub(crate) fn new(ctx_data: Arc<ContextData>, sess: Arc<Session>) -> Self {
        Self {
            ctx_data,
            //SAFETY: see comment on struct, this type owns and manages the session lifetime
            job_session: JobSession::new(unsafe { &*(sess.as_ref() as *const Session) }),
            session: sess,
            transform_data: Vec::new(),
        }
    }
    pub fn update_session(&mut self, sess: Arc<Session>) {
        self.session = sess;
    }
    pub fn run_job(&mut self, job: Job) {
        // SAFETY: see struct comment for why we do this
        // because we marked job_session and transform_data 'static,
        // we normally could not construct the WorkerThreadSession because the
        // &self reference that we have here does not exceed 'static
        // this is fine because we drop wts immediately after, and clear the
        // transform data of all references it may hold while still holding
        // the session arc
        let me = unsafe { std::mem::transmute::<&'_ mut Self, &'static mut Self>(self) };
        let mut wts = WorkerThreadSession {
            transform_data: &mut me.transform_data,
            job_session: &mut me.job_session,
        };
        wts.run_job(job);
        drop(wts);
        self.transform_data.clear();
    }

    pub fn run(&mut self) -> Result<(), ScrError> {
        let mut sess_mgr = self.ctx_data.sess_mgr.lock().unwrap();
        loop {
            if !sess_mgr.terminate {
                if let Some(job) = sess_mgr.job_queue.pop_front() {
                    if !std::ptr::eq(self.session.as_ref(), sess_mgr.session.as_ref()) {
                        self.session = sess_mgr.session.clone();
                    }
                    drop(sess_mgr);
                    self.run_job(job);
                    sess_mgr = self.ctx_data.sess_mgr.lock().unwrap();
                }
            }
            sess_mgr.waiting_worker_threads += 1;
            self.ctx_data.worker_threads_finished.notify_one();
            if sess_mgr.terminate {
                return Ok(());
            }
            sess_mgr = self.ctx_data.jobs_available.wait(sess_mgr).unwrap();
        }
    }
}
