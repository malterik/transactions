use anyhow::Result;
use std::collections::HashMap;

use crate::transaction::{Transaction, TransactionType};

#[derive(Debug)]
pub struct Client {
    available: f32,
    held: f32,
    total: f32,
    locked: bool,
}

#[derive(Debug)]
pub struct TransactionEngine {
    pub clients: HashMap<u16, Client>,
}

impl TransactionEngine {
    pub fn new() -> Result<TransactionEngine> {
        Ok(TransactionEngine {
            clients: HashMap::new(),
        })
    }

    pub fn process(&mut self, transactions: &Vec<Transaction>) {
        for transaction in transactions {
            match transaction.r#type {
                TransactionType::Chargeback => println!("Chargeback"),
                TransactionType::Deposit => handle_deposit(transaction, &mut self.clients),
                TransactionType::Dispute => println!("Dispute"),
                TransactionType::Resolve => println!("Resolve"),
                TransactionType::Withdrawal => handle_withdrawal(transaction, &mut self.clients),
            }
        }
    }
}

fn handle_deposit(transaction: &Transaction, clients: &mut HashMap<u16, Client>) {
    if let Some(client) = clients.get_mut(&transaction.client) {
        client.available += transaction.amount.unwrap();
        client.total += transaction.amount.unwrap();
    } else {
        clients.insert(
            transaction.client,
            Client {
                available: transaction.amount.unwrap(),
                held: 0f32,
                total: transaction.amount.unwrap(),
                locked: false,
            },
        );
    }
}

fn handle_withdrawal(transaction: &Transaction, clients: &mut HashMap<u16, Client>) {
    if let Some(client) = clients.get_mut(&transaction.client) {
        client.available -= transaction.amount.unwrap();
        client.total -= transaction.amount.unwrap();
    } else {
        panic!("Client to withdraw money from does not exist!");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input_parser::InputParser;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_processing_set1() {
        let parser = InputParser::new().unwrap();
        let transactions = parser.parse_transactions("data/set1.csv").await.unwrap();

        let mut engine = TransactionEngine::new().unwrap();
        engine.process(&transactions);
        let c1 = engine.clients.get(&1).unwrap();
        assert_eq!(c1.total, 1.5f32);
        assert_eq!(c1.available, 1.5f32);

        let c2 = engine.clients.get(&2).unwrap();
        assert_eq!(c2.total, -1.0f32);
        assert_eq!(c2.available, -1.0f32);
    }
}
