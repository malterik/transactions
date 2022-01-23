use anyhow::Result;
use core::fmt;
use std::{cmp::Ordering, collections::HashMap};

use crate::transaction::{Transaction, TransactionType};

#[derive(Debug)]
pub struct Client {
    available: f32,
    held: f32,
    total: f32,
    locked: bool,
}

impl fmt::Display for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{},{},{},{}",
            self.available, self.held, self.total, self.locked
        )
    }
}

#[derive(Debug)]
pub struct TransactionEngine {
    pub clients: HashMap<u16, Client>,
    pub dispute_transactions: Vec<Transaction>,
}

impl TransactionEngine {
    pub fn new() -> Result<TransactionEngine> {
        Ok(TransactionEngine {
            clients: HashMap::new(),
            dispute_transactions: Vec::new(),
        })
    }

    pub fn process(&mut self, transactions: &[Transaction]) {
        for transaction in transactions {
            match transaction.r#type {
                TransactionType::Chargeback => handle_chargeback(
                    transaction,
                    &mut self.clients,
                    &mut self.dispute_transactions,
                ),
                TransactionType::Deposit => handle_deposit(transaction, &mut self.clients),
                TransactionType::Dispute => handle_dispute(
                    transaction,
                    &mut self.clients,
                    transactions,
                    &mut self.dispute_transactions,
                ),
                TransactionType::Resolve => handle_resolve(
                    transaction,
                    &mut self.clients,
                    &mut self.dispute_transactions,
                ),
                TransactionType::Withdrawal => handle_withdrawal(transaction, &mut self.clients),
            }
        }
    }

    pub fn print_client_list(&self) {
        println!("client,available,held,total,locked");
        for (id, client) in self.clients.iter() {
            println!("{},{}", id, client);
        }
    }
}

fn handle_deposit(transaction: &Transaction, clients: &mut HashMap<u16, Client>) {
    if let Some(client) = clients.get_mut(&transaction.client) {
        if !client.locked {
            client.available += transaction.amount.unwrap();
            client.total += transaction.amount.unwrap();
        }
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
        if !client.locked {
            client.available -= transaction.amount.unwrap();
            client.total -= transaction.amount.unwrap();
        }
    } else {
        panic!("Client to withdraw money from does not exist!");
    }
}

fn handle_dispute(
    transaction: &Transaction,
    clients: &mut HashMap<u16, Client>,
    transactions: &[Transaction],
    dispute_transactions: &mut Vec<Transaction>,
) {
    if let Some(client) = clients.get_mut(&transaction.client) {
        let transactions_in_dispute: Vec<&Transaction> = transactions
            .iter()
            .filter(|t| {
                t.tx == transaction.tx
                    && (t.r#type == TransactionType::Withdrawal
                        || t.r#type == TransactionType::Deposit)
            })
            .collect();

        match transactions_in_dispute.len().cmp(&1) {
            Ordering::Greater => panic!("Multiple transactions found for dispute!"),
            Ordering::Equal => {
                let transaction_in_dispute = transactions_in_dispute[0];

                if let Some(amount) = transaction_in_dispute.amount {
                    match transaction_in_dispute.r#type {
                        TransactionType::Deposit => {
                            client.available -= amount;
                            client.held += amount;
                        }
                        TransactionType::Withdrawal => {
                            client.available += amount;
                            client.held -= amount;
                        }
                        _ => {
                            // technically it's nowhere written that dispute, resolve and chargeback actions
                            // can't be in dispute themselves but I'm not sure if that's really the case
                            unimplemented!();
                        }
                    }
                }
                dispute_transactions.push(transaction_in_dispute.to_owned());
            }
            _ => (), // Ignore the case that the ID does no exist
        }
    } else {
        panic!("Client to settle dispute for does not exist!");
    }
}

fn handle_resolve(
    transaction: &Transaction,
    clients: &mut HashMap<u16, Client>,
    dispute_transactions: &mut Vec<Transaction>,
) {
    if let Some(client) = clients.get_mut(&transaction.client) {
        let transactions_in_dispute: Vec<&Transaction> = dispute_transactions
            .iter()
            .filter(|t| t.tx == transaction.tx)
            .collect();

        match transactions_in_dispute.len().cmp(&1) {
            Ordering::Greater => panic!("Multiple transactions found for dispute!"),
            Ordering::Equal => {
                let transaction_in_dispute = transactions_in_dispute[0];

                if let Some(amount) = transaction_in_dispute.amount {
                    match transaction_in_dispute.r#type {
                        TransactionType::Deposit => {
                            client.available += amount;
                            client.held -= amount;
                        }
                        TransactionType::Withdrawal => {
                            client.available -= amount;
                            client.held += amount;
                        }
                        _ => {
                            unimplemented!();
                        }
                    }
                } else {
                    panic!("Transaction to resolve was not in dispute")
                }
            }
            _ => (), //  Ignore the case that the ID does no exist
        }
    } else {
        panic!("Client to resolve transacton for does not exist!");
    }
}

fn handle_chargeback(
    transaction: &Transaction,
    clients: &mut HashMap<u16, Client>,
    dispute_transactions: &mut Vec<Transaction>,
) {
    if let Some(client) = clients.get_mut(&transaction.client) {
        let transactions_in_dispute: Vec<&Transaction> = dispute_transactions
            .iter()
            .filter(|t| t.tx == transaction.tx)
            .collect();

        match transactions_in_dispute.len().cmp(&1) {
            Ordering::Greater => panic!("Multiple transactions found for dispute!"),
            Ordering::Equal => {
                let transaction_in_dispute = transactions_in_dispute[0];

                if let Some(amount) = transaction_in_dispute.amount {
                    match transaction_in_dispute.r#type {
                        TransactionType::Deposit => {
                            client.held -= amount;
                            client.total -= amount;
                        }
                        TransactionType::Withdrawal => {
                            client.held += amount;
                            client.total += amount;
                        }
                        _ => {
                            unimplemented!();
                        }
                    }
                } else {
                    panic!("Transaction to resolve was not in dispute")
                }
                client.locked = true;
            }
            _ => (), //  Ignore the case that the ID does no exist
        }
    } else {
        panic!("Client to resolve transacton for does not exist!");
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_processing_set3() {
        let parser = InputParser::new().unwrap();
        let transactions = parser.parse_transactions("data/set3.csv").await.unwrap();

        let mut engine = TransactionEngine::new().unwrap();
        engine.process(&transactions);
        let c1 = engine.clients.get(&1).unwrap();
        assert_eq!(c1.total, 8f32);
        assert_eq!(c1.available, 10f32);
        assert_eq!(c1.held, -2f32);

        let c2 = engine.clients.get(&2).unwrap();
        assert_eq!(c2.total, 8.0f32);
        assert_eq!(c2.available, 2.0f32);
        assert_eq!(c2.held, 6.0f32);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_processing_set4() {
        let parser = InputParser::new().unwrap();
        let transactions = parser.parse_transactions("data/set4.csv").await.unwrap();

        let mut engine = TransactionEngine::new().unwrap();
        engine.process(&transactions);
        let c1 = engine.clients.get(&1).unwrap();
        assert_eq!(c1.total, 11f32);
        assert_eq!(c1.available, 11f32);
        assert_eq!(c1.held, 0f32);

        let c2 = engine.clients.get(&2).unwrap();
        assert_eq!(c2.total, 8.0f32);
        assert_eq!(c2.available, 8.0f32);
        assert_eq!(c2.held, 0.0f32);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_processing_set5() {
        let parser = InputParser::new().unwrap();
        let transactions = parser.parse_transactions("data/set5.csv").await.unwrap();

        let mut engine = TransactionEngine::new().unwrap();
        engine.process(&transactions);
        let c1 = engine.clients.get(&1).unwrap();
        assert_eq!(c1.total, 0f32);
        assert_eq!(c1.available, 0f32);
        assert_eq!(c1.held, 0f32);
        assert_eq!(c1.locked, true);

        let c2 = engine.clients.get(&2).unwrap();
        assert_eq!(c2.total, 8.0f32);
        assert_eq!(c2.available, 8.0f32);
        assert_eq!(c2.held, 0.0f32);
    }
}
