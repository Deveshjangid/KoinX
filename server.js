const express = require('express');
const multer = require('multer');
const csvParser = require('csv-parser');
const mongoose = require('mongoose');
const fs = require('fs');
const path = require('path');
const bodyParser = require('body-parser');

const app = express();
const port = 3000;

mongoose.connect('mongodb+srv://deveshjangidx:PtZrfOIfV8sJsDHn@cluster1.gi1ex0a.mongodb.net/?retryWrites=true&w=majority&appName=Cluster1', {
  useNewUrlParser: true,
  useUnifiedTopology: true,
})
  .then(() => console.log('Connected to MongoDB Atlas'))
  .catch(err => console.error('Failed to connect to MongoDB Atlas', err));

// Define the schema 
const tradeSchema = new mongoose.Schema({
  UTC_Time: Date,
  Operation: String,
  Market: String,
  Base_Coin: String,
  Quote_Coin: String,
  Amount: Number,
  Price: Number,
});

const Trade = mongoose.model('Trade', tradeSchema);

// Middleware 
app.use(bodyParser.json());

// Set up multer 
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, 'uploads/');
  },
  filename: (req, file, cb) => {
    cb(null, file.originalname);
  }
});

const upload = multer({ storage });

// API endpoint to handle CSV file upload
app.post('/upload', upload.single('file'), (req, res) => {
  if (!req.file) {
    return res.status(400).send('No file uploaded.');
  }

  const filePath = req.file.path;
  console.log(`uploaded file path: ${filePath}`);

  // Parseing CSV file 
  const results = [];
 
  fs.createReadStream(filePath)
    .pipe(csvParser())
    .on('data', (data) => {
      const [baseCoin, quoteCoin] = data.Market.split('/');
      const amount = parseFloat(data['Buy / Sell Amount']);
      const price = parseFloat(data.Price);

      if(!isNaN(amount) && !NaN(price)){
        results.push({
          UTC_Time: new Date(data.UTC_Time),
          Operation: data.Operation,
          Market: data.Market,
          Base_Coin: baseCoin,
          Quote_Coin: quoteCoin,
          Amount: parseFloat(data['Buy / Sell Amount']),
          Price: parseFloat(data.Price),
        });
      }else{
        console.warn(`Skipping record with invalid Amount or Price: ${JSON.stringify(data)}`);
      }
    })
    .on('end', () => {
      Trade.insertMany(results)
        .then(() => {
          res.send('File processed and data stored in database');
        })
        .catch((error) => {
          console.error('Error storing data in database', error);
          res.status(500).send('Error storing data in database');
        });
    })
    .on('error', (error) => {
      console.error('Error reading CSV file', error);
      res.status(500).send('Error reading CSV file');
    });
});

// API endpoint to get balance 
app.post('/balance', async (req, res) => {
  const { timestamp } = req.body;
  const date = new Date(timestamp);

  try {
    const trades = await Trade.find({ UTC_Time: { $lte: date } }); 

    const balances = {};

    trades.forEach(trade => {
      const amount = trade.Operation === 'BUY' ? trade.Amount : -trade.Amount;
      if (balances[trade.Base_Coin]) {
        balances[trade.Base_Coin] += amount;
      } else {
        balances[trade.Base_Coin] = amount;
      }
    });

    res.json(balances);
  } catch (error) {
    res.status(500).send('Error fetching trades from database');
  }
});

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`); 
});