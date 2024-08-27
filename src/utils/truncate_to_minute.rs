
use chrono::{DateTime, Utc, Timelike};

// Function to truncate datetime to the minute level
pub fn truncate_to_minute(datetime: &DateTime<Utc>) -> DateTime<Utc> {
    datetime
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap()
}

