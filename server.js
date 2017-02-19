const express = require('express');
const mkdirp = require('mkdirp');
const morgan = require('morgan');
const sharp = require('sharp');
const path = require('path');
const url = require('url');
const fs = require('fs');
const pg = require('pg');

const LargeObjectManager = require('pg-large-object').LargeObjectManager;

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

const cacheDir = process.env.CACHE_DIR || 'tmp';

app.param('filename', function(req, res, next) {
  req.image = path.join(
    cacheDir,
    req.params.model,
    req.params.attribute,
    req.params.id,
    req.params.filename
  );

  next();
});

const oidQuery = function(params) {
  switch (params.model + '_' + params.attribute) {
  case 'picture_image':
    return 'SELECT image AS oid FROM pictures WHERE token = $1 AND image_filename = $2';
  case 'user_avatar':
    return 'SELECT avatar AS oid FROM users WHERE username = $1 AND avatar_filename = $2';
  }
}

const cacheFile = function(req, res, next) {
  fs.stat(req.image, function(err, stats) {
    if (stats) {
      return next();
    }

    mkdirp(path.dirname(req.image), function(err) {
      if (err) {
        return console.error('error creating dir', err);
      }

      pool.connect(function(err, client, done) {
        if (err) {
          return console.error('error fetching client from pool', err);
        }

        const man = new LargeObjectManager(client);

        client.query('BEGIN', function(err, result) {
          if (err) {
            done(err);
            return client.emit('error', err);
          }

          const sql = oidQuery(req.params);

          if (!sql) {
            done();
            console.error('Wrong parameters');
            return next();
          }

          client.query(sql, [req.params.id, req.params.filename], function(err, result) {
            if (err) {
              done(err);
              return client.emit('error', err);
            }

            if (result.rowCount == 0) {
              done();
              console.error('Wrong parameters');
              return next();
            }

            const oid = result.rows[0].oid;

            const bufferSize = 16384;

            man.openAndReadableStream(oid, bufferSize, function(err, size, stream) {
              if (err) {
                done(err);
                return console.error('Unable to read the given large object', err);
              }

              const cache = fs.createWriteStream(req.image);

              stream.pipe(cache);

              // Wait until the stream from the database end to release the
              // connection, then wait until the stream to the cache finish
              // to move on to the next function. Otherwise the resizer will
              // complain about the file :(
              stream.on('end', function() {
                client.query('COMMIT', done);

                cache.on('finish', function() {
                  next();
                });
              });
            });
          });
        });
      });
    });
  });
};

app.get('/:model/:attribute/:id/:geometry/:filename', cacheFile, function(req, res, next) {
  var geometry = req.params.geometry;
  var crop = false;

  if (geometry.slice(-1) == '!') {
    geometry = geometry.slice(0, -1);
    crop = true;
  }

  const dimensions = geometry.split('x');
  const width = Number(dimensions[0]) || null;
  const height = Number(dimensions[1]) || null;

  var resizer = sharp(req.image).resize(width, height);

  if (!crop) {
    resizer = resizer.max()
  }

  resizer
    .sharpen()
    .jpeg({ quality: 90 })
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

app.get('/:model/:attribute/:id/:filename', cacheFile, function(req, res, next) {
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

app.use('/:model/:attribute/:id', function(req, res, next) {
  if (res.image) {
    res.setHeader('Content-Type',   res.image.format);
    res.setHeader('Content-Length', res.image.length);
    res.send(res.image);

    if (req.params.model == 'picture') {
      chargeImage(req.params.id, res.image.length);
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
