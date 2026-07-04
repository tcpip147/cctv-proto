import axios from "axios";
import unzipper from "unzipper";
import fs from "fs";
import path from "path";
import { Readable } from "stream";

interface DownloadOptions {
  url: string;
  targetFiles: string[];
  outputPaths: string[];
}

async function downloadZip(options: DownloadOptions): Promise<void> {
  const { url, targetFiles, outputPaths } = options;

  if (exists(outputPaths)) {
    console.log("No need to download.");
    return;
  }

  try {
    const response = await axios({
      method: "get",
      url: url,
      responseType: "arraybuffer",
    });

    for (let i = 0; i < targetFiles.length; i++) {
      const targetFile = targetFiles[i];
      const outputPath = outputPaths[i];
      const outputDir = path.dirname(outputPath);

      if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir, { recursive: true });
      }

      const zipBuffer = response.data;

      let isFileFound = false;

      await Readable.from(zipBuffer)
        .pipe(unzipper.Parse())
        .on("entry", (entry: unzipper.Entry) => {
          const fileName = entry.path;
          const type = entry.type;

          if (type === "File" && fileName.endsWith(targetFile)) {
            isFileFound = true;
            entry.pipe(fs.createWriteStream(outputPath));
          } else {
            entry.autodrain();
          }
        })
        .promise();
    }
  } catch (error: any) {
    throw error;
  }
}

function exists(outputPaths: string[]): boolean {
  return outputPaths.every((path) => fs.existsSync(path));
}

export { downloadZip };
