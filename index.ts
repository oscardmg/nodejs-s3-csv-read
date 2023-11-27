import {
  S3Client,
  GetObjectCommand,
  ListObjectsV2Command,
} from '@aws-sdk/client-s3';
import { delimiter } from 'path';
import { parse } from 'csv';
import stream from 'stream';


const s3Client = new S3Client({ region: 'us-east-1' }); // replace REGION with your AWS region
const BUCKET_NAME = 'signals-hub-kafka-lambdas-develop-bucket'; // replace BUCKET_NAME with your S3 bucket name

async function listCsvFiles(bucketName: string) {
  const params = {
    Bucket: bucketName,
  };

  try {
    const data = await s3Client.send(new ListObjectsV2Command(params));
    const csvFiles = data?.Contents?.filter((file) =>
      file?.Key?.endsWith('.csv')
    );
    return csvFiles?.map((file) => file.Key);
  } catch (err) {
    console.error('Error in listing S3 files:', err);
    throw err;
  }
}

async function processCsvFile(
  bucketName: string,
  fileName: string
): Promise<void> {
  const command = new GetObjectCommand({ Bucket: bucketName, Key: fileName });
  const response = await s3Client.send(command);
  if (!response.Body) return;
  const s3Stream = response.Body;

  try {
    const { Body } = await s3Client.send(command);

    if (Body instanceof stream.Readable) {
      Body.pipe(parse({
        delimiter: ',',
        columns: true,
    }))
        .on('data', (data) => console.log(data))
        .on('end', () => console.log('Fin de la lectura del CSV.'));
    } else {
      throw new Error('El cuerpo del objeto no es un stream legible.');
    }
  } catch (error) {
    console.error('Error al leer el archivo CSV:', error);
    throw error;
  }
}

async function main(): Promise<void> {
  const csvFiles = await listCsvFiles(BUCKET_NAME);
  if (!csvFiles) return;
  for (const file of csvFiles) {
    if (file) {
      await processCsvFile(BUCKET_NAME, file);
      console.log(file);
    }
  }
}

main().catch(console.error);
