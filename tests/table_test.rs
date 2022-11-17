use bytes::Bytes;
use dwkv::error::Error;
use dwkv::table::{Event, Table, TableOptions};
use dwkv::util::rand_guid;
use rand::seq::SliceRandom;
use std::sync::Arc;
use std::{collections::HashMap, time::Duration};
use tempdir::TempDir;
use tokio::sync::mpsc::Sender;
use tokio::sync::{oneshot, Mutex};

#[tokio::test]
async fn test_table_with_compaction() {
    tracing_subscriber::fmt::init();

    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    let kb = 16;

    let tempdir = TempDir::new("test_dwkv_table").unwrap();
    let table_path = tempdir.path().join("table_1_test");

    tracing::info!("spawning table 1");
    let table_options = TableOptions {
        missing_table_behavior: dwkv::table::MissingTableBehavior::Create,
        writable: true,
        memtable_size_limit: kb * 1024,
        autocompact: true,
    };
    let table_1 = Table::new(&table_path, table_options).await.unwrap();

    let (table_1_sender, table_1_handle) = table_1.spawn().unwrap();

    let key_tracker = Arc::new(Mutex::new(HashMap::<String, usize>::new()));

    let put_count = 5000;
    let unique_key_count = 500;
    let mut all_keys = (0..unique_key_count)
        .map(|i| format!("key/for/{i:0>6}"))
        .cycle()
        .take(put_count)
        .collect::<Vec<_>>();

    all_keys.shuffle(&mut rand::thread_rng());

    let limit = 1000;

    for key in all_keys.iter().take(limit) {
        let count = track_count(&key, &key_tracker).await;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let data: Bytes = data_for_key(&key, count);
        let timestamp = dwkv::util::timestamp();
        tracing::debug!("putting {key} @ {timestamp:?}, iteration = {count}");
        table_1_sender
            .send(Event::Put {
                data,
                timestamp,
                key: key.clone(),
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
            panic!("{e:?}");
        }
    }

    // Checks that if we start a new table at the same path, the data is parseable.
    // FIXME: also check that the write log works in case of a non-clean shutdown.
    let table_2 = Table::new(&table_path, table_options).await.unwrap();
    #[cfg(disabled)]
    let table_3 = Table::new(&table_path.with_file_name("replica"), table_options)
        .await
        .unwrap();

    let (table_2_sender, table_2_handle) = table_2.spawn().unwrap();
    #[cfg(disabled)]
    let (table_3_sender, table_3_handle) = table_3.spawn().unwrap();

    let compaction_sender_2 = table_2_sender.clone();
    #[cfg(disabled)]
    let compaction_sender_3 = table_3_sender.clone();

    tokio::spawn(async move {
        loop {
            compaction_sender_2
                .send(Event::Compact { threshold: 5 })
                .await
                .unwrap();
            #[cfg(disabled)]
            compaction_sender_3
                .send(Event::Compact { threshold: 5 })
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    // should be 49000 keys left
    let share = all_keys.len() / 10;
    let mut handles = vec![];
    for i in 0..10 {
        let sender_2 = table_2_sender.clone();
        // let sender_3 = table_3_sender.clone();
        let mut task_data = all_keys.split_off(share);
        std::mem::swap(&mut task_data, &mut all_keys);

        assert_eq!(task_data.len(), share);
        let tracker = key_tracker.clone();

        handles.push(tokio::spawn(async move {
            tracing::info!("[{i}] starting test task");
            let mut count = 0;
            while let Some(key) = task_data.pop() {
                let guid = format!("{i}:{}", rand_guid(8));
                count += 1;

                if count % 100 == 0 {
                    println!("{count}/{share}");
                }

                let iteration = track_count(&key, &tracker).await;
                let bytes = data_for_key(&key, iteration);
                for sender in [&sender_2].iter() {
                    // dbg!(&count);
                    let (tx, rx) = oneshot::channel();
                    let timestamp = dwkv::util::timestamp();

                    tracing::debug!(
                        "[{guid}] putting {key} @ {timestamp:?}, iteration = {iteration} sender_capacity = {}", sender.capacity()
                    );

                    sender
                        .send(Event::Put {
                            key: key.clone(),
                            timestamp,
                            data: bytes.clone(),
                            reply_to: tx,
                        })
                        .await
                        .unwrap();
                    if let Err(res) = rx.await.unwrap() {
                        tracing::error!("{res:?}");
                    }
                }
            }
        }));
    }

    for (i, handle) in handles.into_iter().enumerate() {
        tracing::info!("Awaiting handle {i}");
        handle.await.unwrap();
    }

    let lock = key_tracker.lock().await;
    for (key, count) in lock.iter() {
        let expected_data = data_for_key(&key, *count);
        let real_data = get_data(&table_2_sender, key).await;
        assert_eq!(real_data, expected_data, "data mismatch on key {key}");
    }

    table_2_sender.send(Event::PrintSelf).await.unwrap();

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
async fn get_data(table_2_sender: &Sender<Event>, key: &str) -> Bytes {
    let (tx, rx) = tokio::sync::oneshot::channel();

    tracing::info!("GET {key}");
    table_2_sender
        .send(Event::Get {
            key: key.to_owned(),
            reply_to: tx,
        })
        .await
        .unwrap();

    let (_ts, res) = rx.await.unwrap().unwrap();

    res
}

fn data_for_key(key: &str, count: usize) -> Bytes {
    let data = format!("this is the data for {key}::{count} ")
        .as_bytes()
        .to_vec();
    let multiplied = data.iter().cycle().take(256);
    let bytes: Bytes = multiplied.copied().collect();

    bytes
}

async fn track_count(key: &str, tracker: &Arc<Mutex<HashMap<String, usize>>>) -> usize {
    let mut lock = tracker.lock().await;
    let entry = lock.entry(key.to_owned()).or_default();
    *entry += 1;
    *entry
}
