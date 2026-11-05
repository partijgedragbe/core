scrape-sessions:
    cargo run --bin sessions

scrape-plenary-meetings *sessions:
    cargo run --release --bin plenary-meetings -- {{sessions}}

scrape-commission-meetings *sessions:
    cargo run --bin commission-meetings -- {{sessions}}

scrape-dossiers:
    cargo run --bin dossiers

scrape-members:
    cargo run --bin members

scrape-lobby:
    cargo run --bin lobby

scrape-remunerations:
    cargo run --bin remunerations

scrape-commissions:
    cargo run --bin commissions

summarize-text:
    cargo run --bin text-summarizer

summarize-dossiers:
    cargo run --bin dossier-summarizer

generate-dossier-markdown:
    python3 summarizers/dossier-pdf-to-markdown/main.py

summarize-pdf-rust:
    cargo run --bin pdf_extractor
