use std::future::Future;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use elura_core::Result;
use elura_core::ownership::Assignment;
use elura_core::session::Identity;
use uuid::Uuid;

/// Immutable Session and routing metadata for an authenticated application request.
#[derive(Debug, Clone)]
pub struct GatewayInterceptContext {
    identity: Identity,
    session_id: Uuid,
    remote_ip: IpAddr,
    trace_id: String,
    ownership: Option<Assignment>,
}

impl GatewayInterceptContext {
    pub(crate) fn new(
        identity: Identity,
        session_id: Uuid,
        remote_ip: IpAddr,
        trace_id: String,
        ownership: Option<Assignment>,
    ) -> Self {
        Self {
            identity,
            session_id,
            remote_ip,
            trace_id,
            ownership,
        }
    }

    pub fn identity(&self) -> &Identity {
        &self.identity
    }

    pub fn session_id(&self) -> Uuid {
        self.session_id
    }

    pub fn remote_ip(&self) -> IpAddr {
        self.remote_ip
    }

    pub fn trace_id(&self) -> &str {
        &self.trace_id
    }

    pub fn ownership(&self) -> Option<&Assignment> {
        self.ownership.as_ref()
    }
}

/// Immutable client request visible to Gateway interceptors.
#[derive(Debug, Clone)]
pub struct GatewayRequest {
    route: u32,
    request_id: u64,
    payload: Bytes,
}

impl GatewayRequest {
    pub(crate) fn new(route: u32, request_id: u64, payload: Bytes) -> Self {
        Self {
            route,
            request_id,
            payload,
        }
    }

    pub fn route(&self) -> u32 {
        self.route
    }

    pub fn request_id(&self) -> u64 {
        self.request_id
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
}

/// Application response returned through the Gateway interceptor chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayResponse {
    payload: Bytes,
}

impl GatewayResponse {
    /// Creates a short-circuit response without calling the remaining chain.
    pub fn new(payload: impl Into<Bytes>) -> Self {
        Self {
            payload: payload.into(),
        }
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub fn into_payload(self) -> Bytes {
        self.payload
    }
}

/// Intercepts authenticated application routes immediately before World dispatch.
///
/// Framework routes such as authentication, heartbeat and reconnect never enter
/// this chain. Context and request values are immutable so an interceptor cannot
/// change identity, routing, ownership or request-deduplication semantics.
#[async_trait]
pub trait GatewayInterceptor: Send + Sync + 'static {
    async fn intercept(
        &self,
        context: &GatewayInterceptContext,
        request: &GatewayRequest,
        next: GatewayNext<'_>,
    ) -> Result<GatewayResponse>;
}

#[async_trait]
impl<T> GatewayInterceptor for Arc<T>
where
    T: GatewayInterceptor + ?Sized,
{
    async fn intercept(
        &self,
        context: &GatewayInterceptContext,
        request: &GatewayRequest,
        next: GatewayNext<'_>,
    ) -> Result<GatewayResponse> {
        self.as_ref().intercept(context, request, next).await
    }
}

#[async_trait]
pub(crate) trait GatewayDispatch: Send + Sync {
    async fn dispatch(
        &self,
        context: &GatewayInterceptContext,
        request: &GatewayRequest,
    ) -> Result<GatewayResponse>;
}

/// Remaining Gateway interceptor chain.
pub struct GatewayNext<'a> {
    interceptors: &'a [Arc<dyn GatewayInterceptor>],
    dispatch: &'a dyn GatewayDispatch,
    context: &'a GatewayInterceptContext,
    request: &'a GatewayRequest,
}

impl<'a> GatewayNext<'a> {
    pub fn run(self) -> Pin<Box<dyn Future<Output = Result<GatewayResponse>> + Send + 'a>> {
        Box::pin(async move {
            match self.interceptors.split_first() {
                Some((interceptor, remaining)) => {
                    interceptor
                        .intercept(
                            self.context,
                            self.request,
                            GatewayNext {
                                interceptors: remaining,
                                dispatch: self.dispatch,
                                context: self.context,
                                request: self.request,
                            },
                        )
                        .await
                }
                None => self.dispatch.dispatch(self.context, self.request).await,
            }
        })
    }
}

pub(crate) async fn run_interceptors(
    interceptors: &[Arc<dyn GatewayInterceptor>],
    dispatch: &dyn GatewayDispatch,
    context: &GatewayInterceptContext,
    request: &GatewayRequest,
) -> Result<GatewayResponse> {
    GatewayNext {
        interceptors,
        dispatch,
        context,
        request,
    }
    .run()
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use elura_core::Error;

    use super::*;

    struct RecordingInterceptor {
        name: &'static str,
        calls: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl GatewayInterceptor for RecordingInterceptor {
        async fn intercept(
            &self,
            _context: &GatewayInterceptContext,
            _request: &GatewayRequest,
            next: GatewayNext<'_>,
        ) -> Result<GatewayResponse> {
            self.calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(format!("{}:before", self.name));
            let response = next.run().await;
            self.calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(format!("{}:after", self.name));
            response
        }
    }

    struct RecordingDispatch(Arc<Mutex<Vec<String>>>);

    #[async_trait]
    impl GatewayDispatch for RecordingDispatch {
        async fn dispatch(
            &self,
            _context: &GatewayInterceptContext,
            request: &GatewayRequest,
        ) -> Result<GatewayResponse> {
            self.0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push("dispatch".into());
            Ok(GatewayResponse::new(request.payload().clone()))
        }
    }

    struct ShortCircuit;

    #[async_trait]
    impl GatewayInterceptor for ShortCircuit {
        async fn intercept(
            &self,
            _context: &GatewayInterceptContext,
            _request: &GatewayRequest,
            _next: GatewayNext<'_>,
        ) -> Result<GatewayResponse> {
            Ok(GatewayResponse::new(Bytes::from_static(b"short")))
        }
    }

    struct Reject;

    #[async_trait]
    impl GatewayInterceptor for Reject {
        async fn intercept(
            &self,
            _context: &GatewayInterceptContext,
            _request: &GatewayRequest,
            _next: GatewayNext<'_>,
        ) -> Result<GatewayResponse> {
            Err(Error::business("REJECTED", "rejected"))
        }
    }

    fn context() -> GatewayInterceptContext {
        GatewayInterceptContext::new(
            Identity {
                account_id: 1,
                user_id: 2,
                region_id: 3,
                realm_id: 4,
                generation: 5,
            },
            Uuid::new_v4(),
            "127.0.0.1".parse().unwrap(),
            "trace".into(),
            None,
        )
    }

    #[tokio::test]
    async fn runs_in_registration_order_and_unwinds_in_reverse() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let interceptors: Vec<Arc<dyn GatewayInterceptor>> = vec![
            Arc::new(RecordingInterceptor {
                name: "one",
                calls: calls.clone(),
            }),
            Arc::new(RecordingInterceptor {
                name: "two",
                calls: calls.clone(),
            }),
        ];
        let dispatch = RecordingDispatch(calls.clone());
        let request = GatewayRequest::new(100, 7, Bytes::from_static(b"payload"));

        let response = run_interceptors(&interceptors, &dispatch, &context(), &request)
            .await
            .unwrap();

        assert_eq!(response.payload(), &Bytes::from_static(b"payload"));
        assert_eq!(
            *calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            [
                "one:before",
                "two:before",
                "dispatch",
                "two:after",
                "one:after"
            ]
        );
    }

    #[tokio::test]
    async fn supports_short_circuit_and_error_responses() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let dispatch = RecordingDispatch(calls.clone());
        let request = GatewayRequest::new(100, 7, Bytes::new());

        let response = run_interceptors(&[Arc::new(ShortCircuit)], &dispatch, &context(), &request)
            .await
            .unwrap();
        assert_eq!(response.payload(), &Bytes::from_static(b"short"));
        assert!(calls.lock().unwrap().is_empty());

        assert!(matches!(
            run_interceptors(&[Arc::new(Reject)], &dispatch, &context(), &request).await,
            Err(Error::Business { .. })
        ));
        assert!(calls.lock().unwrap().is_empty());
    }
}
