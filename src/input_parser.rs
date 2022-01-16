use anyhow::Result;
 
#[derive(Debug)]
pub struct InputParser {
    pub state: u32
}

impl InputParser {
    pub fn new() -> Result<InputParser> {
        Ok(InputParser {
            state: 0
        })
    }
}
