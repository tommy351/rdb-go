//nolint:gosec
package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/go-redis/redis/v7"
	"github.com/phayes/freeport"
)

func main() {
	port, err := freeport.GetFreePort()

	if err != nil {
		panic(err)
	}

	dataDir, err := ioutil.TempDir("", "rdb-fixtures")

	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dataDir)

	redisID, err := startRedisContainer(port, dataDir)

	if err != nil {
		panic(err)
	}

	defer func() {
		_ = stopDockerContainer(redisID)
	}()

	client := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("localhost:%d", port),
	})

	defer client.Close()

	makeRDB(dataDir, client, "quicklist", func(pipe redis.Pipeliner) error {
		var values []interface{}

		for i := 0; i < 100; i++ {
			values = append(values, fmt.Sprintf("v%d", i))
		}

		pipe.RPush("quicklist", values...)
		return nil
	})
}

func startRedisContainer(port int, volume string) (string, error) {
	log.Printf("Starting Redis container (port=%d)", port)

	u, err := user.Current()

	if err != nil {
		return "", err
	}

	var out bytes.Buffer
	cmd := exec.Command("docker", "run",
		"-d",
		"-p", fmt.Sprintf("%d:%d", port, 6379),
		"-v", fmt.Sprintf("%s:/data", volume),
		"--user", fmt.Sprintf("%s:%s", u.Uid, u.Gid),
		"redis:alpine",
	)
	cmd.Stdout = &out

	if err := cmd.Run(); err != nil {
		return "", err
	}

	id := strings.TrimSpace(out.String())

	log.Printf("Redis container started: %s\n", id)
	return id, nil
}

func stopDockerContainer(id string) error {
	log.Printf("Stopping Docker container: %s\n", id)
	return exec.Command("docker", "rm", "-f", id).Run()
}

func makeRDB(dataDir string, client *redis.Client, filename string, fn func(redis.Pipeliner) error) {
	log.Printf("Making RDB: %s\n", filename)

	if err := client.FlushAll().Err(); err != nil {
		panic(err)
	}

	if _, err := client.Pipelined(fn); err != nil {
		panic(err)
	}

	if err := client.Save().Err(); err != nil {
		panic(err)
	}

	src := filepath.Join(dataDir, "dump.rdb")
	dst := filepath.Join("fixtures", filename+".rdb")

	log.Printf("Writing RDB to %s\n", dst)

	if err := copyFile(src, dst); err != nil {
		panic(err)
	}
}

func copyFile(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), os.ModePerm); err != nil {
		return err
	}

	srcFile, err := os.Open(src)

	if err != nil {
		return err
	}

	defer srcFile.Close()

	dstFile, err := os.Create(dst)

	if err != nil {
		return err
	}

	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return err
	}

	return nil
}
