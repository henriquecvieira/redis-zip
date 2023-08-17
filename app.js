import express from "express"
import redis from "redis"
import archiver from "archiver"
import fs from "fs"
import { promises as fsPromises } from "fs"
import { v4 as uuidv4 } from "uuid"
import { promisify } from "util"

const app = express()
app.use(express.json())

const client = redis.createClient({
  host: "localhost", // Coloque o endereço do servidor Redis aqui
  port: 6379, // Coloque a porta do servidor Redis aqui
})

const setAsync = promisify(client.set).bind(client)
const quitAsync = promisify(client.quit).bind(client)

app.post("/saveUser", async (req, res) => {
  try {
    const { nome, email } = req.body
    const id = uuidv4()
    const tempFilePath = `${id}.json`

    await fsPromises.writeFile(
      tempFilePath,
      JSON.stringify({ id, nome, email })
    )

    const zipFilePath = `${id}.zip`
    const output = fs.createWriteStream(zipFilePath)
    const archive = archiver("zip", {
      zlib: { level: 9 },
    })

    archive.pipe(output)
    archive.file(tempFilePath, { name: "usuario.json" })
    archive.finalize()

    await new Promise((resolve) => {
      output.on("close", resolve)
    })

    const zipData = await fsPromises.readFile(zipFilePath)

    await setAsync(id, zipData) // Aguardar a escrita no Redis

    fsPromises.unlink(tempFilePath)
    fsPromises.unlink(zipFilePath)

    quitAsync() // Fechar o cliente Redis após a escrita no Redis

    return res.json({ id, message: "Usuário salvo com sucesso no Redis." })
  } catch (err) {
    console.error(err)
    return res.status(500).json({ error: "Erro interno do servidor." })
  }
})

const PORT = 3001
const startup = async () => {
  await client
    .connect()
    .then(() => {
      console.log("Redis Client Connect")
    })
    .catch((err) => {
      console.log("Redis Client Error:", err)
    })
  client.on("error", (err) => {
    console.error("Redis Client Error:", err)
  })

  app.listen(PORT, () => {
    console.log(`Servidor rodando na porta ${PORT}`)
  })
}
startup()
