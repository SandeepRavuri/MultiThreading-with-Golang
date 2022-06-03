package main

import (
	"container/list"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	progstarttime := time.Now()
	args := os.Args
	mSpawnsStr := args[1]
	filePath := args[2]
	mSpawns, _ := strconv.Atoi(mSpawnsStr)
	avg := coordinator(mSpawns, filePath)
	fmt.Println("Average of numbers in the datafile is:", avg)
	fmt.Println("Time taken to complete the program:", time.Since(progstarttime))
}

func coordinator(spawns int, filePath string) float64 {

	file, err := os.ReadFile("input.txt")

	er(err)

	noOfWorkers := spawns

	fileStats, _ := os.Stat(filePath)

	fileSize := fileStats.Size()

	chnl := make(chan []byte)

	siWkr := fileSize / int64(noOfWorkers)

	siLaWkr := fileSize - (siWkr*int64(noOfWorkers) - 1)

	for i := 0; i < noOfWorkers; i++ {

		if i == noOfWorkers-1 {
			sp := siWkr * int64(i)
			request := workerRequest{filePath, sp, sp + siLaWkr}
			jsonRequest, _ := json.Marshal(request)
			go Worker(i, jsonRequest, chnl)
		} else {
			sp := siWkr * int64(i)
			request := workerRequest{filePath, sp, sp + siWkr}
			jsonRequest, _ := json.Marshal(request)
			go Worker(i, jsonRequest, chnl)
		}

	}

	listS := list.New()

	for i := 0; i < noOfWorkers; i++ {
		x := <-chnl
		listS.PushBack(string(x))
	}

	var numArr []int64
	var totSum int64
	totSum = 0

	for temp := listS.Front(); temp != nil; temp = temp.Next() {
		wrkRes := &workerResponse{}
		json.Unmarshal([]byte(temp.Value.(string)), wrkRes)
		totSum += wrkRes.Psum
		numArr = append(numArr, wrkRes.Numbers...)
	}

	totalnums := strings.Split(string(file), " ")

	var totalnumsIn []int
	for temp := 0; temp < len(totalnums); temp++ {
		str, _ := strconv.Atoi(totalnums[temp])
		totalnumsIn = append(totalnumsIn, int(str))
	}

	count := len(totalnums)

	for temp := 0; temp < len(numArr); temp++ {
		ind := getIndex(totalnumsIn, numArr[temp])
		if ind != -1 {
			totalnumsIn = append(totalnumsIn[:ind], totalnumsIn[ind+1:]...)
		}
	}
	for temp := 0; temp < len(totalnumsIn); temp++ {
		totSum += int64(totalnumsIn[temp])
	}
	fmt.Println("sum of numbers in the datafile:", totSum)
	return float64(totSum) / float64(count)

}

func Worker(id int, data []byte, chnl chan []byte) {
	var jsonData workerRequest
	json.Unmarshal(data, &jsonData)
	flData, _ := os.Open(jsonData.DataFile)

	flData.Seek(jsonData.StartPos, 0)

	getRqudFr := make([]byte, jsonData.EndPos-jsonData.StartPos)
	flData.Read(getRqudFr)

	dataFramnt := string(getRqudFr)

	length := len(dataFramnt)

	var findex int
	var sindex int
	var pre string
	var suf string
	for temp := 0; temp < length; temp++ {
		if string(dataFramnt[temp]) == " " {
			findex = temp
			break
		}
	}

	for temp := 0; temp < length; temp++ {
		if string(dataFramnt[temp]) == " " {
			sindex = temp
		}
	}

	pre = string(dataFramnt[:findex])
	suf = string(dataFramnt[sindex+1:])

	preInt, _ := strconv.Atoi(pre)
	sufInt, _ := strconv.Atoi(suf)

	var midArry []int64
	var number string
	var parsum int64

	for temp := findex + 1; temp <= sindex; temp++ {
		if string(dataFramnt[temp]) == " " {
			finalNum, _ := strconv.Atoi(number)
			midArry = append(midArry, int64(finalNum))
			number = ""
			continue
		}
		number = number + string(dataFramnt[temp])
	}

	for temp := 0; temp < len(midArry); temp++ {
		parsum += int64(midArry[temp])
	}

	response := workerResponse{parsum, int64(len(midArry)), int64(preInt), int64(sufInt), int64(jsonData.StartPos), int64(jsonData.EndPos), midArry}
	jsonResponse, _ := json.Marshal(response)

	chnl <- jsonResponse

}

func getIndex(arr []int, val int64) int {
	for i := 0; i < len(arr); i++ {
		if int64(arr[i]) == val {
			return i
		}
	}
	return -1
}

func er(err error) {
	if err != nil {
		panic(err)
	}
}

type workerRequest struct {
	DataFile string
	StartPos int64
	EndPos   int64
}

type workerResponse struct {
	Psum    int64
	PCount  int64
	Prefix  int64
	suffix  int64
	StPos   int64
	EndPos  int64
	Numbers []int64
}
