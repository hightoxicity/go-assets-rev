package main

import (
  "fmt"
  "os"
  "hash/crc32"
  "encoding/hex"
  "io"
  "io/ioutil"
  "encoding/json"
  "sync"
  "time"
  "strings"
  "regexp"
  "path/filepath"
  "flag"
)

type Filepathspec struct {
  Src string `json:"src"`
  Dest string `json:"dest"`
  Crc32 string `json:"crc32"`
  Size int64 `json:"size"`
}

type Config struct {
  RootDir string `json:"root_dir"`
  OutputFilepath string `json:"output_filepath"`
  FileFilter string `json:"file_filter"`
  DestFormat string `json:"dest_format"`
  OutputMode string `json:"output_mode"`
}

func CountSubdirsRecursively(dirPath string) (int, error) {
  var result = 0
  dir, err := os.Open(dirPath)
  if err != nil {
    fmt.Println(err)
    return 0, nil
  }

  defer dir.Close()
  dirInfo, err := dir.Stat()
  if err != nil {
    return 0, err
  }

  if (!dirInfo.IsDir()) {
    return 0, err
  }

  result++

  subDirs, err := ioutil.ReadDir(dirPath)
  if err != nil {
    return 0, err
  }

  for _, sd := range subDirs {
    if (sd.IsDir()) {
      subCount, err := CountSubdirsRecursively(dirPath + "/" + sd.Name())
      if err != nil {
        return 0, err
      }
      result += subCount
    } else {
      var fName = dirPath + "/" + sd.Name()
      if (sd.Mode() & os.ModeSymlink != 0) {
        resLink, err := os.Readlink(fName)
        if err != nil {
        } else {
          if (strings.HasPrefix(resLink, "/")) {
            fName = resLink
          } else {
            fName = dirPath + "/" + resLink
          }

          // If symlink is a directory
          fSymTarget, err := os.Open(fName)
          if err != nil {
          } else {
            defer fSymTarget.Close()
            symTargetInfo, err := fSymTarget.Stat()
            if err != nil {
            } else {
              if symTargetInfo.IsDir() {
                subCount, err := CountSubdirsRecursively(fName + "/")
                if err != nil {
                  subCount = 0
                }
                result += subCount
              }
            }
          }
        }
      }
    }
  }

  return result, nil
}

func HashFileCrc32(filePath string, polynomial uint32) (string, error) {
  var returnCRC32String string

  file, err := os.Open(filePath)
  if err != nil {
    return returnCRC32String, err
  }
  defer file.Close()
  tablePolynomial := crc32.MakeTable(polynomial)
  hash := crc32.New(tablePolynomial)
  if _, err := io.Copy(hash, file); err != nil {
    return returnCRC32String, err
  }
  hashInBytes := hash.Sum(nil)[:]
  returnCRC32String = hex.EncodeToString(hashInBytes)

  return returnCRC32String, nil
}

func ScanTree(filePath string, absPath string, filter string, destFormat string, ch chan Filepathspec, procch chan int, errs chan error) {
  if filter == "" {
    filter = ".*"
  }

  if destFormat == ""{
    destFormat = "%srcdir%%srcfilename%.%crc32%%srcext%"
  }

  if strings.HasSuffix(filePath, "/") {
    filePath = strings.TrimRight(filePath, "/")
  }

  file, err := os.Open(filePath)

  if err != nil {
    errs <- err
    return
  }
  defer file.Close()

  filesInfo, err := file.Readdir(-1)

  if err != nil {
    errs <- err
  }

  for i := 0; i < len(filesInfo); i++ {
    if (filesInfo[i].IsDir()) {
      go ScanTree(filePath + "/" + filesInfo[i].Name(), absPath + filesInfo[i].Name() + "/", filter, destFormat, ch, procch, errs)
    } else {
      var fName = filePath + "/" + filesInfo[i].Name()
      if (filesInfo[i].Mode() & os.ModeSymlink != 0) {
        resLink, err := os.Readlink(fName)
        if err != nil {
          errs <- err
          continue
        } else {
          if (strings.HasPrefix(resLink, "/")) {
            fName = resLink
          } else {
            fName = filePath + "/" + resLink
          }

          // If symlink is a directory
          fSymTarget, err := os.Open(fName)
          if err != nil {
            errs <- err
            continue
          }
          defer fSymTarget.Close()
          symTargetInfo, err := fSymTarget.Stat()
          if err != nil {
            errs <- err
            continue
          }
          if symTargetInfo.IsDir() {
            go ScanTree(fName, absPath + filesInfo[i].Name() + "/", filter, destFormat, ch, procch, errs)
            continue
          }
        }
      }

      filterMatched, err := regexp.MatchString(filter, filesInfo[i].Name())
      if (err != nil) {
        errs <- err
        continue
      }
      if !filterMatched {
        continue
      }

      hash, err := HashFileCrc32(fName, 0xedb88320)
      if err != nil {
        errs <- err
        continue
      }

      destStr := destFormat
      if strings.Contains(destStr, "%srcdir%") {
        destStr = strings.Replace(destStr, "%srcdir%", absPath, -1)
      }

      cExt := strings.Contains(destStr, "%srcext%")
      cSrcFilename := strings.Contains(destStr, "%srcfilename%")
      if cSrcFilename || cExt {
        ext := filepath.Ext(filesInfo[i].Name())

        if cExt {
          destStr = strings.Replace(destStr, "%srcext%", ext, -1)
        }

        if cSrcFilename {
          var srcFilename string
          if ext == "" {
            srcFilename = filesInfo[i].Name()
          } else {
            srcFilename = strings.TrimSuffix(filesInfo[i].Name(), ext)
          }
          destStr = strings.Replace(destStr, "%srcfilename%", srcFilename, -1)
        }
      }

      if strings.Contains(destStr, "%crc32%") {
        destStr = strings.Replace(destStr, "%crc32%", hash, -1)
      }

      //fmtime := filesInfo[i].ModTime()
      //fmtime.Format(time.RFC1123Z)
      //fmt.Sprintf("\"%xT-%xO\"", fmtime.Unix(), filesInfo[i].Size())

      ch <- Filepathspec{Src: absPath + filesInfo[i].Name(), Crc32: hash, Size: filesInfo[i].Size(), Dest: destStr}
    }
  }
  procch <- 1

  return
}

func JsonOutputManager(destFile string, doAppend bool, ch chan Filepathspec, errs chan error, wg *sync.WaitGroup) {
  defer wg.Done()

  var err error
  var fp *os.File
  var fPathSps []Filepathspec

  if doAppend {
    fileData, e := ioutil.ReadFile(destFile)
    if e != nil {
      fmt.Printf("%v\n", e)
    } else {
      err := json.Unmarshal(fileData, &fPathSps)

      if err != nil {
        fmt.Printf("%v\n", err)
      }
    }
  }

  fp, err = os.Create(destFile)

  if err != nil {
    errs <- err
    return
  }
  defer fp.Close()

  fp.WriteString("[\n")

  var i = 0

  for {
    select {
      case f, more := <- ch:
        if (more) {
          buffer, _ := json.MarshalIndent(f, "  ", "  ")
          if i > 0 {
            fp.WriteString(",\n")
          }
          fp.WriteString("  ")
          fp.Write(buffer)
          i++
        } else {
          for j, sps := range fPathSps {
            buffer, _ := json.MarshalIndent(sps, "  ", "  ")
            if i > 0 || j > 0 {
              fp.WriteString(",\n")
            }
            fp.WriteString("  ")
            fp.Write(buffer)
          }

          fmt.Println("No more things to write!")
          fp.WriteString("\n]")
          return
        }
    }
  }

  return
}

func GenCrcFileList(jsonConf Config, chanBufferSize int) {
  count, err := CountSubdirsRecursively(jsonConf.RootDir)
  if (err != nil) {
    fmt.Println(err)
  } else {
    fmt.Printf("We will process %d directories\n", count)
  }

  ch := make(chan Filepathspec, chanBufferSize)
  procch := make(chan int, chanBufferSize)
  errs := make(chan error, chanBufferSize)

  var outwg sync.WaitGroup

  outwg.Add(1)

  append := (jsonConf.OutputMode == "append")

  go JsonOutputManager(jsonConf.OutputFilepath, append, ch, errs, &outwg)

  go ScanTree(jsonConf.RootDir, "/", jsonConf.FileFilter, jsonConf.DestFormat, ch, procch, errs)

  n := 0

  for n < count {
    select {
      case pr := <- procch:
        n += pr
      case e := <-errs:
        fmt.Println(e)
    }
  }
  close(procch)
  close(ch)
  close(errs)

  outwg.Wait()
}

func main() {
  configPath := flag.String("config", "/etc/file-crc32-calculator/config.json", "JSON Config Filepath")
  bufferSize := flag.Int("channels_buf_size", 5, "Size of channels buffers")
  flag.Parse()
  fmt.Println("Config used: ", *configPath)

  confFile, e := ioutil.ReadFile(*configPath)
  if e != nil {
    fmt.Printf("JSON config filepath error: %v\n", e)
    os.Exit(1)
  }

  var jsonConfs []Config
  err := json.Unmarshal(confFile, &jsonConfs)

  if err != nil {
    fmt.Printf("JSON config error: %v\n", err)
    os.Exit(1)
  }

  start := time.Now()

  for _, cf := range jsonConfs {
    GenCrcFileList(cf, *bufferSize)
  }

  elapsed := time.Since(start)

  fmt.Println("All done!")
  fmt.Printf("It tooks %v to run.\n", elapsed)
}
