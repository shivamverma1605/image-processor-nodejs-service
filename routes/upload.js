const express = require('express');
const multer = require('multer');
const csv = require('csv-parser');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');
const Request = require('../models/Request');
const Product = require('../models/Product');

const router = express.Router();
const upload = multer({ dest: 'uploads/' });

// Overall, this API endpoint is responsible for receiving a file upload, checking if a file was uploaded, and returning an error response if no file is found.
// If a file is found, it reads the file as a stream and parses the CSV data. It then creates a new request in the database and saves the products from the CSV data.
// The inputImageUrls are split by commas and trimmed to remove any extra whitespace. The file is then deleted from the server.
// If an error occurs during the process, a 500 Internal Server Error response is sent.
router.post('/', upload.single('file'), async (req, res) => {
    try {
        const file = req.file;
        if (!file) {
            return res.status(400).json({ error: 'No file uploaded' });
        }

        const requestId = uuidv4();
        const request = new Request({ id: requestId });
        await request.save();

        const products = [];
        fs.createReadStream(file.path)
            .pipe(csv())
            .on('data', (row) => {
                const { 'S. No.': serialNumber, 'Product Name': productName, 'Input Image Urls': inputImageUrls } = row;
                const product = new Product({
                    requestId,
                    serialNumber: parseInt(serialNumber),
                    productName,
                    inputImageUrls: inputImageUrls.split(',').map(url => url.trim())
                });
                products.push(product);
            })
            .on('end', async () => {
                await Product.insertMany(products);
                res.status(202).json({ requestId });
                fs.unlinkSync(file.path); // delete the uploaded file
            });
    } catch (err) {
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

module.exports = router;