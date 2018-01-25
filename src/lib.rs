extern crate batch_recv;
extern crate crossbeam_channel as chan;
extern crate futures;
extern crate itertools;
extern crate worker_sentinel;

use std::fmt::Debug;
use futures::sync::oneshot;
use itertools::Itertools;
use worker_sentinel::{Work, WorkFactory};
use batch_recv::BatchRecv;

pub trait Value: Debug + Clone + Send {
    type Key: Ord + Clone + Send + 'static;
    fn key(&self) -> &Self::Key;
}

pub trait Backend: Send + 'static {
    type Value: Value;
    type Error: Debug + Clone + Send;
    fn batch_load<'a, I>(&self, keys: I) -> Result<Vec<Self::Value>, Self::Error>
    where
        I: Iterator<Item = &'a <Self::Value as Value>::Key> + 'a;
}

pub trait NewBackend: Send + Sync + 'static {
    type Backend: Backend;
    fn new_backend(&self) -> Self::Backend;
}
impl<F, B> NewBackend for F
where
    B: Backend,
    F: Fn() -> B + Send + Sync + 'static,
{
    type Backend = B;
    fn new_backend(&self) -> Self::Backend {
        self()
    }
}

type LoadResult<B> = Result<Option<<B as Backend>::Value>, <B as Backend>::Error>;
type Message<B> = (
    <<B as Backend>::Value as Value>::Key,
    oneshot::Sender<LoadResult<B>>,
);
type QueueTx<B> = chan::Sender<Message<B>>;
type QueueRx<B> = chan::Receiver<Message<B>>;

#[derive(Clone)]
pub struct Loader<B>
where
    B: Backend,
{
    queue_tx: QueueTx<B>,
}

impl<B> Loader<B>
where
    B: Backend,
{
    pub fn new<N>(new_backend: N, batch_size: usize, concurrent: usize) -> Loader<B>
    where
        N: NewBackend<Backend = B> + 'static,
    {
        let (queue_tx, queue_rx) = chan::unbounded();
        let work_factory = BackendWorkFactory {
            queue_rx,
            new_backend,
            batch_size,
        };
        worker_sentinel::spawn(concurrent, work_factory);
        Loader { queue_tx }
    }

    pub fn load(
        &self,
        key: <B::Value as Value>::Key,
    ) -> Result<oneshot::Receiver<LoadResult<B>>, chan::SendError<<B::Value as Value>::Key>> {
        let (cb_tx, cb_rx) = oneshot::channel();
        self.queue_tx.send((key, cb_tx)).map_err(|err| {
            let (key, _) = err.into_inner();
            chan::SendError(key)
        })?;
        Ok(cb_rx)
    }
}

struct BackendWork<B>
where
    B: Backend,
{
    queue_rx: QueueRx<B>,
    backend: B,
    batch_size: usize,
}
impl<B> Work for BackendWork<B>
where
    B: Backend,
{
    fn work(self) -> Option<Self> {
        let mut requests: Vec<_> = self.queue_rx.batch_recv(self.batch_size).ok()?.collect();
        requests.sort_by(|&(ref left, _), &(ref right, _)| left.cmp(&right));
        let req_groups_by_key = requests.into_iter().group_by(|&(ref key, _)| key.clone());
        let req_groups_by_key_vec: Vec<_> = req_groups_by_key.into_iter().collect();

        let ret = {
            let keys_iter = req_groups_by_key_vec.iter().map(|&(ref key, _)| key);
            self.backend.batch_load(keys_iter)
        };
        let mut values = match ret {
            Ok(values) => values,
            Err(err) => {
                for (_, req_group) in req_groups_by_key_vec {
                    for (_, cb) in req_group {
                        cb.send(Err(err.clone())).expect("return error as result");
                    }
                }
                return Some(self);
            }
        };
        values.sort_by(|ref left, ref right| left.key().cmp(right.key()));
        let joined = req_groups_by_key_vec
            .into_iter()
            .merge_join_by(values.into_iter(), |&(ref key, _), value| {
                key.cmp(value.key())
            });
        for pair in joined {
            use itertools::EitherOrBoth::{Both, Left};
            match pair {
                Left((_, req_group)) => for (_, cb) in req_group {
                    cb.send(Ok(None)).expect("respond to caller");
                },
                Both((_, req_group), value) => for (_, cb) in req_group {
                    cb.send(Ok(Some(value.clone()))).expect("respond to caller");
                },
                _ => unreachable!(),
            }
        }
        Some(self)
    }
}

struct BackendWorkFactory<N>
where
    N: NewBackend,
{
    queue_rx: QueueRx<N::Backend>,
    new_backend: N,
    batch_size: usize,
}
impl<N> WorkFactory for BackendWorkFactory<N>
where
    N: NewBackend,
{
    type Work = BackendWork<N::Backend>;
    fn build(&self) -> Self::Work {
        let backend = self.new_backend.new_backend();
        let queue_rx = self.queue_rx.clone();
        let batch_size = self.batch_size;
        BackendWork {
            backend,
            queue_rx,
            batch_size,
        }
    }
}

#[cfg(test)]
mod teet {
    use futures::{Future, future};
    use super::{Backend, Loader, Value};
    #[derive(Debug, Clone, PartialEq)]
    struct HalfValue {
        key: u32,
        half: u32,
    }
    impl Value for HalfValue {
        type Key = u32;
        fn key(&self) -> &u32 {
            &self.key
        }
    }
    struct HalfBackend;
    impl Backend for HalfBackend {
        type Value = HalfValue;
        type Error = ();
        fn batch_load<'a, I>(&self, keys: I) -> Result<Vec<Self::Value>, Self::Error>
        where
            I: Iterator<Item = &'a <Self::Value as Value>::Key> + 'a,
        {
            let ret = keys.filter_map(|&key| {
                if key % 2 == 0 {
                    Some(HalfValue { key, half: key / 2 })
                } else {
                    None
                }
            }).collect();
            Ok(ret)
        }
    }

    #[test]
    fn test_loader() {
        let loader = Loader::new(|| HalfBackend, 10, 1);

        let f1 = loader.load(1).unwrap().map(|v| assert!(v.unwrap().is_none()));
        let f2 = loader.load(2).unwrap().map(|v| assert_eq!(v.unwrap().unwrap(), HalfValue { key: 2, half: 1 }));
        let f3 = loader.load(3).unwrap().map(|v| assert!(v.unwrap().is_none()));
        let f4 = loader.load(4).unwrap().map(|v| assert_eq!(v.unwrap().unwrap(), HalfValue { key: 4, half: 2 }));
        future::join_all(vec![
            Box::new(f1) as Box<Future<Item = _, Error = _>>,
            Box::new(f2) as Box<_>,
            Box::new(f3) as Box<_>,
            Box::new(f4) as Box<_>,
        ]).wait().unwrap();
    }
}
