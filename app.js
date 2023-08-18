import fs from 'fs';
import express from 'express';
import multer from 'multer';
import JSONStream from 'JSONStream';
import JSZip from 'jszip';
import path from 'path';
import { Readable } from 'stream';
import Queue from 'bull';
import BullBoard from 'bull-board';
import Redis from 'ioredis';

const app = express();
app.use(express.json());

// Configuração do cliente Redis
const redisConfig = {
  host: '127.0.0.1', // Coloque o host correto do seu Redis
  port: 6379, // Coloque a porta correta do seu Redis
};

// Crie um cliente Redis
const redisClient = new Redis(redisConfig);

const minhaFila = "minha-fila"

// Crie uma instância da fila usando Bull
const queue = new Queue(minhaFila, {
  createClient: () => redisClient, // Passa o cliente Redis para a fila
});



// Configurar o armazenamento dos arquivos com o Multer
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, 'uploads/');
  },
  filename: (req, file, cb) => {
    cb(null, `${Date.now()}-${file.originalname}`);
  },
});

const upload = multer({ storage });

// Nome do arquivo zipado
let zipFilePath = '';

// Rota para enviar um arquivo ZIP
app.post('/upload', upload.single('document'), (req, res) => {
  zipFilePath = 'uploads/' + req.file.filename;
  processZipFile(zipFilePath);
  res.json({ message: 'Arquivo ZIP enviado com sucesso!' + req.file.filename });
});


// Configura o BullBoard para usar a fila
BullBoard.setQueues([queue]);

// Rota para o dashboard
app.use('/dashboard', BullBoard.UI);


// Função para adicionar um item à fila
async function adicionarItemAFila(data) {
  // Adiciona o item à fila
  await queue.add(data);

  console.log(`Item adicionado à fila: ${JSON.stringify(data)}`);
}



// Crie uma função para ler e processar o arquivo JSON zipado
async function processZipFile(zipFilePath) {
  const filePath = path.resolve(zipFilePath);
  const readStream = fs.createReadStream(filePath);
  const jszip = new JSZip();

  const chunks = [];
  readStream.on('data', (chunk) => chunks.push(chunk));
  readStream.on('end', async () => {
    const buffer = Buffer.concat(chunks);
    const zip = await jszip.loadAsync(buffer);

    const zipObjectKeys = Object.keys(zip.files);
    for (const key of zipObjectKeys) {
      if (zip.files[key].dir) continue; // Ignorar diretórios
      const fileData = await zip.files[key].async('string');
      const jsonStream = JSONStream.parse('*');
      const jsonReadStream = new Readable();
      jsonReadStream.push(fileData);
      jsonReadStream.push(null);

      jsonReadStream.pipe(jsonStream).on('data', async (data) => {
        await adicionarItemAFila(data)
        console.log(data);
      });
    }

    console.log('Leitura do arquivo JSON concluída.');
  });

  readStream.on('error', (error) => {
    console.error('Erro ao ler o arquivo:', error);
  });

}



app.listen(3333, () => {
  console.log(`Servidor rodando na porta 3333`);
});
