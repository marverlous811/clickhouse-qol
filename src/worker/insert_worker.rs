use std::time::Duration;

use clickhouse::{Client, Row};
use serde::Serialize;
use tokio::sync::mpsc::{Receiver, Sender};

pub enum WorkerCommand<T>
where
    T: Row + Serialize,
{
    Insert(T),
    Ping,
}

pub struct StreamInsertWorker<T>
where
    T: Row + Serialize,
{
    rx: Receiver<WorkerCommand<T>>,
    client: Client,
    max_size: u64,
    period_sec: u64,
    table_name: String,
}

impl<T> StreamInsertWorker<T>
where
    T: Row + Serialize,
{
    pub fn new(client: Client, table_name: String, max_size: u64, period_sec: u64) -> (Self, Sender<WorkerCommand<T>>) {
        let (tx, rx) = tokio::sync::mpsc::channel::<WorkerCommand<T>>(1024);
        let worker = StreamInsertWorker {
            rx,
            client,
            max_size,
            period_sec,
            table_name,
        };
        (worker, tx)
    }

    pub async fn recv(&mut self) -> anyhow::Result<()> {
        let mut current = 0;
        let mut inserter = self
            .client
            .inserter(&self.table_name)?
            .with_max_bytes(100_000)
            .with_max_rows(self.max_size)
            .with_period(Some(Duration::from_secs(self.period_sec)));

        while let Some(command) = self.rx.recv().await {
            match command {
                WorkerCommand::Insert(data) => {
                    let row = data;
                    match inserter.write(&row) {
                        Ok(_) => current += 1,
                        Err(e) => {
                            log::error!("failed to write: {:?}", e);
                        }
                    }

                    if current >= self.max_size {
                        match inserter.commit().await {
                            Ok(res) => {
                                log::info!("commit result: {:?}", res);
                                current = 0;
                            }
                            Err(e) => {
                                log::error!("failed to commit: {:?}", e);
                            }
                        }
                    }
                }
                WorkerCommand::Ping => {
                    if let Some(left) = inserter.time_left() {
                        if left.as_millis() <= 100 {
                            match inserter.commit().await {
                                Ok(res) => {
                                    log::info!("commit result: {:?}", res);
                                    current = 0;
                                }
                                Err(e) => {
                                    log::error!("failed to commit: {:?}", e);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
