const express = require('express');
const mongoose = require('mongoose');
const { mongoURI } = require('./config');
const uploadRouter = require('./routes/upload');
const statusRouter = require('./routes/status');
const webhookRouter = require('./routes/webhook');

const app = express();
app.use(express.json());

mongoose.connect(mongoURI)
    .then(() => console.log('MongoDB connected'))
    .catch(err => console.log(err));

app.get('/get-csv', (req, res) => {
    res.download('./output.csv');
});
app.use('/upload', uploadRouter);
app.use('/status', statusRouter);
app.use('/webhook', webhookRouter);

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));