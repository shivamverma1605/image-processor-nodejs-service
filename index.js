const express = require('express');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const { parse } = require('csv-parse');
const axios = require('axios');
const { Pool } = require('pg');

const app = express();
const port = 3000;

const pool = new Pool({
    user: 'your_db_user',
    host: 'localhost',
    database: 'image_processing',
    password: 'your_db_password',
    port: 5432,
});

// Database Schema Initialization
async function initDb() {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS requests (
            request_id UUID PRIMARY KEY,
            status VARCHAR(50),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
    `);

    await pool.query(`
        CREATE TABLE IF NOT EXISTS products (
            product_id SERIAL PRIMARY KEY,
            request_id UUID REFERENCES requests(request_id),
            product_name VARCHAR(255),
            input_image_urls TEXT[],
            output_image_urls TEXT[]
        );
    `);
}

initDb();

app.use(express.json());

// Upload API
app.post('/upload', async (req, res) => {
    const requestId = uuidv4();
    const { csvData } = req.body;

    try {
        await pool.query('INSERT INTO requests (request_id, status) VALUES ($1, $2)', [requestId, 'Pending']);

        const records = [];
        parse(csvData, { columns: true, trim: true }, async (err, rows) => {
            if (err) {
                console.error(err);
                return res.status(400).json({ error: 'Invalid CSV format' });
            }

            for (const row of rows) {
                const inputImageUrls = row['Input Image Urls'].split(',').map(url => url.trim());
                records.push(pool.query(
                    'INSERT INTO products (request_id, product_name, input_image_urls) VALUES ($1, $2, $3)',
                    [requestId, row['Product Name'], inputImageUrls]
                ));
            }

            await Promise.all(records);
            await processImages(requestId);
        });

        res.json({ requestId });
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

// Image Processing Simulation
async function processImages(requestId) {
    const { rows } = await pool.query('SELECT * FROM products WHERE request_id = $1', [requestId]);

    for (const product of rows) {
        const outputImageUrls = product.input_image_urls.map(url => `${url.replace('.jpg', '-compressed.jpg')}`);
        await pool.query('UPDATE products SET output_image_urls = $1 WHERE product_id = $2', [outputImageUrls, product.product_id]);
    }

    await pool.query('UPDATE requests SET status = $1 WHERE request_id = $2', ['Completed', requestId]);
    await triggerWebhook(requestId);
}

// Status API
app.get('/status/:requestId', async (req, res) => {
    const { requestId } = req.params;

    const { rows: requestRows } = await pool.query('SELECT * FROM requests WHERE request_id = $1', [requestId]);
    if (requestRows.length === 0) return res.status(404).json({ error: 'Request not found' });

    const { rows: productRows } = await pool.query('SELECT * FROM products WHERE request_id = $1', [requestId]);

    res.json({
        status: requestRows[0].status,
        products: productRows.map(product => ({
            productName: product.product_name,
            inputImageUrls: product.input_image_urls,
            outputImageUrls: product.output_image_urls || []
        }))
    });
});

// Webhook Trigger
async function triggerWebhook(requestId) {
    try {
        await axios.post('https://webhook-endpoint.example.com', { requestId, status: 'Completed' });
    } catch (error) {
        console.error('Failed to trigger webhook:', error);
    }
}

app.listen(port, () => {
    console.log(`Server running on http://localhost:${port}`);
});
