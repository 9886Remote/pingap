use super::file_appender::new_rolling_file_writer;
use super::LOG_TARGET;
use async_nats::Client as NatsClient;
use async_trait::async_trait;
use bytes::BytesMut;
use pingap_core::Error;
use pingora::server::ShutdownWatch;
use pingora::services::background::BackgroundService;
use serde::{Deserialize, Serialize};
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex;
use tracing::{error, info, warn};
use tracing_appender::rolling::RollingFileAppender;

type Result<T> = std::result::Result<T, Error>;

/// NATS 日志任务配置参数
#[derive(Debug, PartialEq, Deserialize, Serialize, Default)]
struct NatsLoggerParams {
    /// NATS 主题名称，日志将发送到此主题
    subject: Option<String>,
    /// 通道缓冲区大小，控制内存中待发送日志数量
    channel_buffer: Option<usize>,
    /// 批量发送大小，达到此数量后立即发送
    batch_size: Option<usize>,
    /// 刷新超时时间，即使未达到批量大小也会发送
    #[serde(default)]
    #[serde(with = "humantime_serde")]
    flush_timeout: Option<Duration>,
    /// 本地降级文件路径，当 NATS 不可用时写入本地文件
    fallback: Option<String>,
    /// NATS 连接超时时间
    #[serde(default)]
    #[serde(with = "humantime_serde")]
    connect_timeout: Option<Duration>,
    /// 是否启用失败重试
    retry_enabled: Option<bool>,
    /// 最大重试次数
    max_retries: Option<u32>,
    /// 重连延迟时间
    #[serde(default)]
    #[serde(with = "humantime_serde")]
    reconnect_delay: Option<Duration>,
    /// 健康检查间隔
    #[serde(default)]
    #[serde(with = "humantime_serde")]
    health_check_interval: Option<Duration>,
}

/// NATS 日志任务结构
pub struct NatsLoggerTask {
    /// NATS 服务器地址
    nats_url: String,
    /// 消息接收器
    receiver: Mutex<Option<Receiver<BytesMut>>>,
    /// 配置参数
    params: NatsLoggerParams,
    /// 本地降级写入器（可选）
    fallback_writer: Mutex<Option<BufWriter<RollingFileAppender>>>,
    /// 统计：已成功发送的消息数
    messages_sent: AtomicU64,
    /// 统计：发送失败的消息数
    messages_failed: AtomicU64,
    /// 统计：重连次数
    reconnect_count: AtomicU32,
}

impl NatsLoggerTask {
    /// 获取 NATS 主题名称
    pub fn get_subject(&self) -> String {
        self.params
            .subject
            .clone()
            .unwrap_or_else(|| "pingap.logs".to_string())
    }

    /// 获取统计信息
    pub fn get_stats(&self) -> (u64, u64, u32) {
        (
            self.messages_sent.load(Ordering::Relaxed),
            self.messages_failed.load(Ordering::Relaxed),
            self.reconnect_count.load(Ordering::Relaxed),
        )
    }

    /// 连接到 NATS 服务器
    async fn connect_nats(&self) -> Result<NatsClient> {
        let timeout = self
            .params
            .connect_timeout
            .unwrap_or(Duration::from_secs(5));

        let connect_future = async_nats::connect(&self.nats_url);

        match tokio::time::timeout(timeout, connect_future).await {
            Ok(Ok(client)) => {
                let (sent, failed, reconnects) = self.get_stats();
                info!(
                    target: LOG_TARGET,
                    url = self.nats_url,
                    subject = self.get_subject(),
                    messages_sent = sent,
                    messages_failed = failed,
                    reconnect_count = reconnects,
                    "connected to NATS server successfully"
                );
                Ok(client)
            },
            Ok(Err(e)) => Err(Error::Invalid {
                message: format!("Failed to connect to NATS: {}", e),
            }),
            Err(_) => Err(Error::Invalid {
                message: format!("NATS connection timeout after {:?}", timeout),
            }),
        }
    }

    /// 健康检查：验证 NATS 连接是否正常
    async fn health_check(&self, client: &NatsClient) -> bool {
        // 使用 flush 来检查连接健康状态
        match tokio::time::timeout(Duration::from_secs(2), client.flush()).await
        {
            Ok(Ok(_)) => true,
            Ok(Err(e)) => {
                warn!(
                    target: LOG_TARGET,
                    error = %e,
                    "NATS health check failed"
                );
                false
            },
            Err(_) => {
                warn!(
                    target: LOG_TARGET,
                    "NATS health check timeout"
                );
                false
            },
        }
    }

    /// 批量发送日志到 NATS
    async fn send_batch_to_nats(
        &self,
        client: &NatsClient,
        messages: &[BytesMut],
    ) -> Result<()> {
        let subject = self.get_subject();
        let retry_enabled = self.params.retry_enabled.unwrap_or(true);
        let max_retries = self.params.max_retries.unwrap_or(3);
        let mut failed_count = 0;

        for msg in messages.iter() {
            // 发送到 NATS，移除尾部换行符因为 NATS 消息本身就是分离的
            let payload = msg.trim_ascii_end();

            let mut attempt = 0;
            let mut success = false;

            while attempt <= max_retries {
                // 使用 subject.as_str() 避免每次循环克隆
                match client.publish(subject.as_str(), payload.into()).await {
                    Ok(_) => {
                        success = true;
                        break;
                    },
                    Err(e) => {
                        attempt += 1;
                        if retry_enabled && attempt <= max_retries {
                            warn!(
                                target: LOG_TARGET,
                                error = %e,
                                attempt,
                                max_retries,
                                "failed to publish to NATS, retrying..."
                            );
                            // 指数退避：100ms * attempt
                            tokio::time::sleep(Duration::from_millis(
                                100 * attempt as u64,
                            ))
                            .await;
                        } else {
                            error!(
                                target: LOG_TARGET,
                                error = %e,
                                attempt,
                                max_retries,
                                retry_enabled,
                                "failed to publish to NATS"
                            );
                            break;
                        }
                    },
                }
            }

            if !success {
                failed_count += 1;
            }
        }

        // 更新统计信息
        let success_count = messages.len() - failed_count;
        if success_count > 0 {
            self.messages_sent
                .fetch_add(success_count as u64, Ordering::Relaxed);
        }
        if failed_count > 0 {
            self.messages_failed
                .fetch_add(failed_count as u64, Ordering::Relaxed);
            return Err(Error::Invalid {
                message: format!(
                    "Failed to send {} out of {} messages to NATS",
                    failed_count,
                    messages.len()
                ),
            });
        }

        Ok(())
    }

    /// 写入降级文件
    async fn write_to_fallback(&self, messages: &[BytesMut]) -> Result<()> {
        let mut writer_guard = self.fallback_writer.lock().await;
        if let Some(writer) = writer_guard.as_mut() {
            for msg in messages.iter() {
                // 写入消息内容
                if let Err(e) = writer.write_all(msg) {
                    error!(
                        target: LOG_TARGET,
                        error = %e,
                        "failed to write to fallback file"
                    );
                    return Err(Error::Io { source: e });
                }

                // 确保有换行符
                if !msg.ends_with(b"\n") {
                    if let Err(e) = writer.write_all(b"\n") {
                        error!(
                            target: LOG_TARGET,
                            error = %e,
                            "failed to write newline to fallback file"
                        );
                        return Err(Error::Io { source: e });
                    }
                }
            }

            // 刷新到磁盘
            if let Err(e) = writer.flush() {
                error!(
                    target: LOG_TARGET,
                    error = %e,
                    "failed to flush fallback file"
                );
                return Err(Error::Io { source: e });
            }

            info!(
                target: LOG_TARGET,
                count = messages.len(),
                "wrote messages to fallback file"
            );
        } else {
            warn!(
                target: LOG_TARGET,
                count = messages.len(),
                "no fallback writer configured, messages may be lost"
            );
        }
        Ok(())
    }
}

pub async fn new_nats_logger(
    nats_url: &str,
) -> Result<(Sender<BytesMut>, NatsLoggerTask)> {
    // 解析 URL：移除 nats:// 前缀并分离地址和查询参数
    let url_without_scheme =
        nats_url
            .strip_prefix("nats://")
            .ok_or_else(|| Error::Invalid {
                message: "NATS URL must start with 'nats://'".to_string(),
            })?;

    let (addr, query) = url_without_scheme
        .split_once('?')
        .unwrap_or((url_without_scheme, ""));

    // 解析查询参数
    let params: NatsLoggerParams =
        serde_qs::from_str(query).map_err(|e| Error::Invalid {
            message: format!("Failed to parse NATS parameters: {}", e),
        })?;

    // 验证必需参数
    if params.subject.is_none() {
        return Err(Error::Invalid {
            message: "NATS subject is required (add ?subject=your.topic)"
                .to_string(),
        });
    }

    // 重新构造 NATS URL
    let nats_url = format!("nats://{}", addr);

    // 初始化降级文件写入器（如果配置）
    let fallback_writer = if let Some(fallback_path) = &params.fallback {
        info!(
            target: LOG_TARGET,
            path = fallback_path,
            "initializing fallback file writer"
        );

        match new_rolling_file_writer(fallback_path) {
            Ok(rolling_writer) => {
                info!(
                    target: LOG_TARGET,
                    path = fallback_path,
                    "fallback file writer initialized successfully"
                );
                Some(BufWriter::new(rolling_writer.writer))
            },
            Err(e) => {
                warn!(
                    target: LOG_TARGET,
                    error = %e,
                    path = fallback_path,
                    "failed to create fallback writer, continuing without fallback"
                );
                None
            },
        }
    } else {
        None
    };

    let channel_buffer = params.channel_buffer.unwrap_or(1000);

    let (tx, rx) = channel::<BytesMut>(channel_buffer);

    let task = NatsLoggerTask {
        nats_url,
        receiver: Mutex::new(Some(rx)),
        params,
        fallback_writer: Mutex::new(fallback_writer),
        messages_sent: AtomicU64::new(0),
        messages_failed: AtomicU64::new(0),
        reconnect_count: AtomicU32::new(0),
    };

    Ok((tx, task))
}

#[async_trait]
impl BackgroundService for NatsLoggerTask {
    async fn start(&self, mut shutdown: ShutdownWatch) {
        let Some(mut receiver) = self.receiver.lock().await.take() else {
            error!(
                target: LOG_TARGET,
                "receiver already taken, cannot start NATS logger"
            );
            return;
        };

        // 尝试连接 NATS
        let mut current_client = match self.connect_nats().await {
            Ok(c) => Some(c),
            Err(e) => {
                error!(
                    target: LOG_TARGET,
                    error = %e,
                    "failed to connect to NATS initially, will use fallback only"
                );
                None
            },
        };

        let batch_size = self.params.batch_size.unwrap_or(128);
        let flush_timeout =
            self.params.flush_timeout.unwrap_or(Duration::from_secs(10));
        let reconnect_delay = self
            .params
            .reconnect_delay
            .unwrap_or(Duration::from_secs(2));
        let health_check_interval = self
            .params
            .health_check_interval
            .unwrap_or(Duration::from_secs(30));

        info!(
            target: LOG_TARGET,
            url = self.nats_url,
            subject = self.get_subject(),
            channel_buffer = self.params.channel_buffer.unwrap_or(1000),
            batch_size,
            flush_timeout = format!("{:?}", flush_timeout),
            reconnect_delay = format!("{:?}", reconnect_delay),
            health_check_interval = format!("{:?}", health_check_interval),
            has_fallback = self.params.fallback.is_some(),
            nats_connected = current_client.is_some(),
            "NATS logger is running"
        );

        let mut flush_interval = tokio::time::interval(flush_timeout);
        let mut health_check_interval =
            tokio::time::interval(health_check_interval);

        // 跳过首次立即触发
        flush_interval.tick().await;
        health_check_interval.tick().await;

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    info!(
                        target: LOG_TARGET,
                        "received shutdown signal, draining logs..."
                    );
                    break;
                }
                Some(msg) = receiver.recv() => {
                    // 批量收集消息
                    let mut messages = Vec::with_capacity(batch_size);
                    messages.push(msg);

                    while messages.len() < batch_size {
                        match receiver.try_recv() {
                            Ok(msg) => messages.push(msg),
                            Err(_) => break,
                        }
                    }

                    // 尝试发送到 NATS
                    if let Some(ref client) = current_client {
                        match self.send_batch_to_nats(client, &messages).await {
                            Ok(_) => {
                                // 发送成功，继续
                            }
                            Err(e) => {
                                warn!(
                                    target: LOG_TARGET,
                                    error = %e,
                                    batch_size = messages.len(),
                                    "failed to send batch to NATS, will reconnect and use fallback"
                                );

                                // 标记需要重连
                                current_client = None;

                                // 写入降级文件
                                if let Err(e) = self.write_to_fallback(&messages).await {
                                    error!(
                                        target: LOG_TARGET,
                                        error = %e,
                                        "failed to write to fallback, messages lost"
                                    );
                                }
                            }
                        }
                    } else {
                        // NATS 不可用，只写入降级文件
                        if let Err(e) = self.write_to_fallback(&messages).await {
                            error!(
                                target: LOG_TARGET,
                                error = %e,
                                "failed to write to fallback, messages may be lost"
                            );
                        }

                        // 尝试重连（带延迟）
                        info!(
                            target: LOG_TARGET,
                            delay = format!("{:?}", reconnect_delay),
                            "attempting to reconnect to NATS..."
                        );
                        tokio::time::sleep(reconnect_delay).await;

                        match self.connect_nats().await {
                            Ok(c) => {
                                self.reconnect_count.fetch_add(1, Ordering::Relaxed);
                                current_client = Some(c);
                                info!(
                                    target: LOG_TARGET,
                                    "reconnected to NATS successfully"
                                );
                            }
                            Err(e) => {
                                error!(
                                    target: LOG_TARGET,
                                    error = %e,
                                    "reconnection failed, will retry later"
                                );
                            }
                        }
                    }
                }
                _ = flush_interval.tick() => {
                    // 定期刷新降级文件
                    if let Some(writer) = self.fallback_writer.lock().await.as_mut() {
                        if let Err(e) = writer.flush() {
                            error!(
                                target: LOG_TARGET,
                                error = %e,
                                "failed to flush fallback file"
                            );
                        }
                    }
                }
                _ = health_check_interval.tick() => {
                    // 定期健康检查
                    if let Some(ref client) = current_client {
                        if !self.health_check(client).await {
                            warn!(
                                target: LOG_TARGET,
                                "NATS health check failed, marking for reconnection"
                            );
                            current_client = None;
                        }
                    }
                }
                else => {
                    // recv() 返回 None，所有发送者都已关闭
                    info!(
                        target: LOG_TARGET,
                        "all senders closed, stopping NATS logger"
                    );
                    break;
                }
            }
        }

        info!(
            target: LOG_TARGET,
            "draining remaining messages from channel..."
        );

        let mut remaining_messages = Vec::new();
        while let Some(msg) = receiver.recv().await {
            remaining_messages.push(msg);
        }

        if !remaining_messages.is_empty() {
            info!(
                target: LOG_TARGET,
                count = remaining_messages.len(),
                "processing remaining messages..."
            );

            // 尝试发送剩余消息到 NATS
            if let Some(ref client) = current_client {
                if let Err(e) =
                    self.send_batch_to_nats(client, &remaining_messages).await
                {
                    warn!(
                        target: LOG_TARGET,
                        error = %e,
                        "failed to send remaining messages to NATS"
                    );
                }
            }

            // 确保写入降级文件
            if let Err(e) = self.write_to_fallback(&remaining_messages).await {
                error!(
                    target: LOG_TARGET,
                    error = %e,
                    "failed to write remaining messages to fallback"
                );
            }
        }

        // 最终刷新降级文件
        if let Some(writer) = self.fallback_writer.lock().await.as_mut() {
            if let Err(e) = writer.flush() {
                error!(
                    target: LOG_TARGET,
                    error = %e,
                    "failed to flush fallback file on shutdown"
                );
            }
        }

        // 输出最终统计
        let (sent, failed, reconnects) = self.get_stats();
        info!(
            target: LOG_TARGET,
            messages_sent = sent,
            messages_failed = failed,
            reconnect_count = reconnects,
            "NATS logger stopped gracefully"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_nats_params() {
        let query = "subject=app.logs&batch_size=50&channel_buffer=2000&retry_enabled=true&max_retries=5";
        let params: NatsLoggerParams = serde_qs::from_str(query).unwrap();

        assert_eq!(params.subject, Some("app.logs".to_string()));
        assert_eq!(params.batch_size, Some(50));
        assert_eq!(params.channel_buffer, Some(2000));
        assert_eq!(params.retry_enabled, Some(true));
        assert_eq!(params.max_retries, Some(5));
    }

    #[test]
    fn test_parse_nats_url() {
        let url = "nats://localhost:4222?subject=logs&batch_size=100";
        let stripped = url.strip_prefix("nats://").unwrap();
        let (addr, query) = stripped.split_once('?').unwrap();

        assert_eq!(addr, "localhost:4222");
        assert_eq!(query, "subject=logs&batch_size=100");
    }

    #[test]
    fn test_nats_url_without_query() {
        let url = "nats://localhost:4222";
        let stripped = url.strip_prefix("nats://").unwrap();
        let (addr, query) = stripped.split_once('?').unwrap_or((stripped, ""));

        assert_eq!(addr, "localhost:4222");
        assert_eq!(query, "");
    }

    #[test]
    fn test_invalid_url_format() {
        let url = "http://localhost:4222?subject=logs";
        let result = url.strip_prefix("nats://");

        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_new_nats_logger_missing_subject() {
        let url = "nats://localhost:4222";
        let result = new_nats_logger(url).await;

        assert!(result.is_err());
        if let Err(Error::Invalid { message }) = result {
            assert!(message.contains("subject is required"));
        }
    }

    #[tokio::test]
    async fn test_new_nats_logger_invalid_scheme() {
        let url = "http://localhost:4222?subject=logs";
        let result = new_nats_logger(url).await;

        assert!(result.is_err());
        if let Err(Error::Invalid { message }) = result {
            assert!(message.contains("must start with 'nats://'"));
        }
    }
}
