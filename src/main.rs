use std::{collections::HashMap, time::Duration};

use bytes::Bytes;
use error::Error;
use sstable::rand_bytes;
use table::{Event, Table};
use tokio::sync::oneshot;

pub mod error;
pub mod index;
pub mod sstable;
pub mod table;

#[tokio::main]
async fn main() {
    fixme("make this an integration test");
    tracing_subscriber::fmt::init();
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    let mb = 32;

    tracing::info!("spawning table 1");
    let table_1 = Table::new("/Users/zach/data/table_1_test", mb * 1024 * 1024)
        .await
        .unwrap();

    let (table_1_sender, table_1_handle) = table_1.spawn().unwrap();

    let mut generated_data = HashMap::new();

    let limit = 1000;

    for i in 0..limit {
        tracing::debug!("put {i:0>6}");
        let (tx, rx) = tokio::sync::oneshot::channel();
        // let data = rand_bytes(24);
        let data = format!("this is the data for {i}").as_bytes().to_vec();
        let multiplied = data.iter().cycle().take(64 * 1024);
        let bytes: Bytes = multiplied.copied().collect();
        generated_data.insert(i, bytes.clone());
        table_1_sender
            .send(Event::Put {
                data: bytes,
                timestamp: sstable::timestamp(),
                key: format!("key/for/{i:0>6}"),
                reply_to: tx,
            })
            .await
            .unwrap();

        rx.await.unwrap().unwrap();
    }

    table_1_sender.send(Event::Shutdown).await.unwrap();
    match table_1_handle.await.unwrap() {
        Ok(_) => {}
        Err(Error::OkShutdown) => {}
        Err(e) => {
            tracing::error!("{e:?}");
        }
    }

    // tokio::time::sleep(Duration::from_secs(100)).await;

    let table_2 = Table::new("/Users/zach/data/table_1_test", mb * 1024 * 1024)
        .await
        .unwrap();

    let (table_2_sender, table_2_handle) = table_2.spawn().unwrap();

    let compaction_sender = table_2_sender.clone();
    tokio::spawn(async move {
        loop {
            compaction_sender.send(Event::Compact).await.unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    });

    for i in 0..limit {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let key = format!("key/for/{i:0>6}");
        tracing::info!("GET {key}");
        table_2_sender
            .send(Event::Get { key, reply_to: tx })
            .await
            .unwrap();

        let res = rx.await.unwrap().unwrap();
        assert_eq!(res, generated_data.get(&i).unwrap(), "error on {i}");
    }

    for i in 5..8 {
        let sender = table_2_sender.clone();
        tokio::spawn(async move {
            loop {
                let rand_id = u16::from_be_bytes(rand_bytes(2).as_ref().try_into().unwrap());
                let (tx, rx) = oneshot::channel();
                let data = format!("this is the data for {rand_id}")
                    .as_bytes()
                    .to_vec();
                let multiplied = data.iter().cycle().take(16 * 1024);
                let bytes: Bytes = multiplied.copied().collect();
                sender
                    .send(Event::Put {
                        key: format!("key/for/{i}{rand_id:0>5}"),
                        data: bytes,
                        timestamp: sstable::timestamp(),
                        reply_to: tx,
                    })
                    .await
                    .unwrap();
                if let Err(res) = rx.await.unwrap() {
                    tracing::error!("{res:?}");
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
    }

    tokio::time::sleep(Duration::from_secs(300)).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    table_2_sender.send(Event::Shutdown).await.unwrap();
    match table_2_handle.await.unwrap() {
        Ok(_) | Err(Error::OkShutdown) => {
            tracing::info!("bye!")
        }
        Err(e) => {
            tracing::error!("{e:?}");
        }
    }
}

#[deprecated = "FIXME"]
pub fn fixme<T>(arg: T) -> T {
    arg
}

#[deprecated = "FIXME"]
pub fn fixme_msg<T>(arg: T, _msg: &'static str) -> T {
    arg
}
