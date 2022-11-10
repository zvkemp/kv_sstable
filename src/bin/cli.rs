use std::{collections::HashMap, path::PathBuf};

use dwkv::fixme;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    fixme("list-tables");
    fixme("allow listing for all shards");

    let nodes = vec![
        "/Users/zach/workspace/bucket-summarizer-0",
        "/Users/zach/workspace/bucket-summarizer-1",
        "/Users/zach/workspace/bucket-summarizer-2",
        "/Users/zach/workspace/bucket-summarizer-3",
        "/Users/zach/workspace/bucket-summarizer-4",
    ];

    let shard_count = 128;

    #[derive(Debug)]
    struct ShardIndexEntry {
        node_id: usize,
        shard_id: usize,
        index_path: PathBuf,
    }

    todo!("FIXME: this needs a rewrite.");
    let mut index_paths = HashMap::<String, Vec<ShardIndexEntry>>::new();
    for (node_id, node) in nodes.iter().enumerate() {
        for shard_id in 0..shard_count {
            let path = PathBuf::from(node.to_string()).join(shard_id.to_string());
            let mut readdir = tokio::fs::read_dir(&path).await.unwrap();

            while let Some(entry) = readdir.next_entry().await.unwrap() {
                let table_name = entry
                    .path()
                    .file_name()
                    .map(|x| x.to_str().unwrap())
                    .unwrap()
                    .to_owned();
                if table_name.as_str() == ".DS_Store" {
                    continue;
                }

                let hashmap_entry = index_paths.entry(table_name).or_default();

                let mut table_entries = tokio::fs::read_dir(entry.path()).await.unwrap();

                while let Some(table_entry) = table_entries.next_entry().await.unwrap() {
                    if table_entry.path().extension().map(|x| x.to_str().unwrap()) == Some("index")
                    {
                        hashmap_entry.push(ShardIndexEntry {
                            node_id,
                            shard_id,
                            index_path: table_entry.path().to_owned(),
                        })
                    }
                }
            }
        }
    }

    // println!("{:#?}", index_paths);

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
                .map(|entry| entry.index_path.as_path())
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
