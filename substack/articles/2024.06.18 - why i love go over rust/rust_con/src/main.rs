use reqwest;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

async fn fetch_url(semaphore: Arc<Semaphore>, url: &str) {
    let permit = semaphore.acquire().await.unwrap();
    match reqwest::get(url).await {
        Ok(response) => {
            let body = response.text().await.unwrap_or_else(|_| "Failed to read response body".to_string());
            let output = if body.len() > 25 { &body[..25] } else { &body };
            println!("Response from URL {}: {}", url, output);
        }
        Err(err) => {
            println!("Failed to fetch URL {}: {:?}", url, err);
        }
    }
    drop(permit);
}

#[tokio::main]
async fn main() {
    let urls = vec![
        "http://www.google.com",
        "http://www.yahoo.com",
        "http://www.espn.com",
        "http://www.engadget.com",
        "http://www.space.com",
    ];

    let semaphore = Arc::new(Semaphore::new(5));
    let mut tasks = vec![];

    for url in &urls {
        let semaphore_clone = semaphore.clone();
        let url_clone = url.to_string();
        let task = tokio::spawn(async move {
            fetch_url(semaphore_clone, &url_clone).await;
        });
        tasks.push(task);
    }

    for task in tasks {
        task.await.unwrap();
    }

    println!("All web requests completed.");
}
