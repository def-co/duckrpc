use std::fmt::Debug;
use std::io::{stdin, Stdin, stdout, Stdout};
use std::pin::pin;

use thiserror::Error;
use duckdb::Connection;

mod eval;
mod input;
mod output;

use input::Command;
use output::Response;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    JsonError(#[from] json::Error),

    #[error(transparent)]
    DbError(#[from] duckdb::Error),

    #[error("eof")]
    Eof,

    #[error("{}", .0)]
    General(&'static str),
}

pub type Result<T> = std::result::Result<T, Error>;

fn err<T>(msg: &'static str) -> Result<T> {
    Err(Error::General(msg))
}

struct Server<'a> {
    stdin: &'a mut Stdin,
    stdout: &'a mut Stdout,
    db: Connection,
}

impl<'a> Server<'a> {
    fn do_loop(&mut self) -> Result<()> {
        output::write_response(self.stdout, Response::Ok)?;

        loop {
            let cmd = match input::read_command(self.stdin) {
                Err(Error::Eof) => break,
                Err(err) => {
                    output::write_response(self.stdout, Response::Error(err))?;
                    continue;
                },
                Ok(cmd) => cmd,
            };

            match self.do_cmd(cmd) {
                Ok(()) => (),
                Err(err) => output::write_response(self.stdout, Response::Error(err))?,
            }
        }

        Ok(())
    }

    fn do_cmd(&mut self, cmd: Command) -> Result<()> {
        let resp = match cmd {
            Command::Execute(query) => {
                let affected_rows = eval::execute(&self.db, query)?;
                Response::Done { affected_rows }
            },
            Command::QueryImmediate(query) => {
                let mut sh = pin!(eval::prepare(&self.db, &query)?);
                sh.as_mut().query(&query)?;
                let (stride, values) = sh.as_mut().fetch_rows(usize::MAX)?;
                let col_names = sh.as_ref().col_names();
                Response::QueryRows {
                    col_names: Some(col_names),
                    stride: stride as u32,
                    values,
                }
            },
            Command::QueryHandle(_) => todo!(),
            Command::QueryRow => todo!(),
            Command::QueryRows(_) => todo!(),
            Command::AppendPrepare(_) => todo!(),
            Command::AppendRow(_, _) => todo!(),
            Command::AppendRows(_, _) => todo!(),
            Command::AppendFlush => todo!(),
        };
        output::write_response(self.stdout, resp)?;
        Ok(())
    }
}

fn do_main(r#in: &mut Stdin, r#out: &mut Stdout) -> Result<()> {
    let db = {
        let mut args = std::env::args();
        if args.len() < 2 {
            return err("not enough args");
        }

        Connection::open(args.nth(1).unwrap())?
    };

    let mut serv = Server {
        stdin: r#in,
        stdout: r#out,
        db,
    };
    serv.do_loop()
}

fn main() {
    let mut r#in = stdin();
    let mut r#out = stdout();

    do_main(&mut r#in, &mut r#out).map_err(|err| {
        output::write_response(&mut r#out, Response::Error(err)).unwrap();
        std::process::exit(1);
    }).unwrap();
}
