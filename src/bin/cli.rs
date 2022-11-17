use std::{collections::HashMap, path::PathBuf};

use dwkv::fixme;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    fixme("list-tables");
    fixme("allow listing for all shards");

    let nodes = vec![
        "/Users/zach/data/cluster_a/node0",
        "/Users/zach/data/cluster_a/node1",
        "/Users/zach/data/cluster_a/node2",
        "/Users/zach/data/cluster_a/node3",
        "/Users/zach/data/cluster_a/node4",
    ];

    let shard_count = 128;

    #[derive(Debug)]
    struct ShardIndexEntry {
        node_id: usize,
        shard_id: usize,
        sstable_dir: PathBuf,
    }

    let mut index_paths = HashMap::<String, Vec<ShardIndexEntry>>::new();
    for (node_id, node) in nodes.iter().enumerate() {
        for shard_id in 0..shard_count {
            let path = PathBuf::from(node.to_string()).join(shard_id.to_string());
            if !path.is_dir() {
                println!("shard not found at {:?}", path);
                continue;
            }

            let mut readdir = tokio::fs::read_dir(&path).await.unwrap();

            while let Some(entry) = readdir.next_entry().await.unwrap() {
                if entry.path().is_dir() {
                    let mut read_dir_inner = tokio::fs::read_dir(entry.path()).await.unwrap();

                    while let Some(table_entry) = read_dir_inner.next_entry().await.unwrap() {
                        if table_entry.path().is_dir() {
                            let table_name = table_entry
                                .path()
                                .file_name()
                                .map(|x| x.to_str().unwrap())
                                .unwrap()
                                .to_owned();

                            let hashmap_entry = index_paths.entry(table_name).or_default();

                            let mut sstable_entries =
                                tokio::fs::read_dir(table_entry.path()).await.unwrap();
                            while let Some(sstable_entry) =
                                sstable_entries.next_entry().await.unwrap()
                            {
                                if sstable_entry.path().join("sstable").is_file() {
                                    {
                                        hashmap_entry.push(ShardIndexEntry {
                                            node_id,
                                            shard_id,
                                            sstable_dir: sstable_entry.path(),
                                        })
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    println!("{:#?}", index_paths);

    match args.get(1).map(|s| s.as_ref()) {
        Some("list-keys") => {
            // println!("listing keys...");
            let table_path = args.get(2).unwrap();

            // let table = Table::readonly(&table_path).await.unwrap();
            // let mut keys = table.list_keys().await;

            let paths = index_paths
                .get(table_path)
                .unwrap()
                .iter()
                .filter_map(|entry| {
                    let path = entry.sstable_dir.as_path().join("index");

                    if path.is_file() {
                        Some(path)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            for path in paths.iter() {
                println!("{path:?}");
            }

            let mut keys = dwkv::util::list_keys_in_indexes(paths.into_iter()).await;

            while let Some(key) = keys.next().await {
                println!("{:?}", key);
            }
        }

        Some("list-tables") => {
            let mut keys = index_paths.keys().cloned().collect::<Vec<_>>();
            keys.sort();
            keys.dedup();

            for key in keys {
                println!("{key}");
            }
        }
        _ => {
            todo!()
        }
    }
}
