const express = require('express');
const morgan = require('morgan');
const sharp = require('sharp');
const path = require('path');
const url = require('url');
const fs = require('fs');
const pg = require('pg');

if (typeof process.env.DATABASE_URL === 'undefined') {
  throw new ReferenceError('DATABASE_URL environment variable must be set');
}
const params = url.parse(process.env.DATABASE_URL);
const auth = params.auth.split(':');
const config = {
  user: auth[0],
  password: auth[1],
  host: params.hostname,
  port: params.port,
  database: params.pathname.split('/')[1],
};
const pool = new pg.Pool(config);
const query = 'UPDATE users SET balance = balance - $1 FROM pictures WHERE token = $2 AND users.id = pictures.user_id';
const chargeImage = function(token, length) {
  pool.connect(function(err, client, done) {
    if (err) {
      return console.error('error fetching client from pool', err);
    }
    client.query(query, [length, token], function(err, result) {
      done(); // release the client back to the pool

      if (err) {
        return console.error('error running query', err);
      }
    });
  });
};

const app = express();

app.use(morgan('combined'))

const root = process.env.PUBLIC_DIR || 'public';
app.param('filename', function(req, res, next) {
  req.image = path.join(
    root,
    req.params.prefix,
    req.params.model,
    req.params.attribute,
    req.params.token,
    req.params.filename
  );

  next();
});

app.get('/:prefix/:model/:attribute/:token/:geometry/:filename', function(req, res, next) {
  const dimensions = req.params.geometry.split('x');
  const width = Number(dimensions[0]) || null;
  const height = Number(dimensions[1]) || null;

  sharp(req.image)
    .resize(width, height)
    .max() // TODO: remove if geometry ends with '!'
    .jpeg({ quality: 80 })
    .toBuffer(function(err, data, info) {
      if (err) {
        next(err);
        return console.error('error resizing image ' + req.image, err);
      }

      res.image = data;
      res.image.format = info.format;
      next();
    })
});

app.get('/:prefix/:model/:attribute/:token/:filename', function(req, res, next) {
  fs.readFile(req.image, function(err, data) {
    if (err) {
      next(err);
      return console.error('error reading image ' + req.image, err);
    }

    res.image = data;
    res.image.format = 'image/jpeg';
    next();
  })
})

app.use('/:prefix/:model/:attribute/:token', function(req, res, next) {
  if (res.image) {
    res.setHeader('Content-Type',   res.image.format);
    res.setHeader('Content-Length', res.image.length);
    res.send(res.image);

    if (req.params.model == 'picture') {
      chargeImage(req.params.token, res.image.length);
    }
  } else {
    next();
  }
});

app.use(express.static('public'));

const errorPage = fs.readFileSync(path.join(__dirname, 'public/index.html'));

app.use(function(req, res) {
  res.writeHead(404, { 'Content-Type': 'text/html' });
  res.write(errorPage);
  res.end();
});

app.use(function(err, req, res, next) {
  // FIXME: should be 500?
  res.writeHead(404, { 'Content-Type': 'text/html' });
  res.write(errorPage);
  res.end();
});

app.listen(process.env.PORT || '3000', function() {
  //console.log('Serving');
})
