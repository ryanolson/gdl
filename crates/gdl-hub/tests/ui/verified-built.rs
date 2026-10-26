// A verified report cannot be built outside the report's verification.
use gdl_cells::Withdrawal;
use gdl_watch::Report;

fn main() {
    let report = Report::Withdrawal(Withdrawal {
        class: "model_x".into(),
        cell: "model-x-01".into(),
        epoch: 1,
        seq: 1,
    });
    let _ = gdl_hub::VerifiedReport(report);
}
