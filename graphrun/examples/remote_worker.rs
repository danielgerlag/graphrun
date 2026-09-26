use graphrun::{ActivityError, Catalog, TlsMaterial, Worker};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 6 {
        return Err(
            "usage: remote_worker https://host:port ca.pem worker.pem worker.key server-name"
                .into(),
        );
    }
    let catalog = Catalog::from_json(
        br#"{"format":"graphrun.catalog/v1","schemas":{},"activities":[
            {"name":"sample.increment","version":1,"input_schema":"integer/v1",
             "output_schema":"integer/v1","execution":"async","effects":"pure",
             "recovery":"RetrySafe","error_codes":[]}
        ]}"#,
    )?;
    let tls = TlsMaterial {
        ca_pem: std::fs::read_to_string(&args[2])?,
        cert_pem: std::fs::read_to_string(&args[3])?,
        key_pem: std::fs::read_to_string(&args[4])?,
        server_name: args[5].clone(),
    };
    Worker::builder(args[1].clone(), tls, catalog)
        .activity("sample.increment", 1, |value: i64, ctx| async move {
            if !ctx.can_start_effect() {
                return Err(ActivityError::new("worker.stopping", "claim is stopping"));
            }
            Ok(value + 1)
        })?
        .open()
        .await?
        .run()
        .await?;
    Ok(())
}
