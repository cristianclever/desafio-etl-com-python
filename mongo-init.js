db = db.getSiblingDB('news_db');
db.createUser({
    user: 'news_db_rw',
    pwd: 'CrazyFish',
    roles: [
        {role: 'readWrite', db: 'news_db'}
    ]
});


//Criacao de todas as collections e indices:::::
db.createCollection("news");
db.createCollection("tickers");