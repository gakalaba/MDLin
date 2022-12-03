package stats

import (
  "encoding/json"
  "io/ioutil"
  "log"
  "strconv"
  "sync"
)

type StatsMap struct {
  stats map[string]interface{}
  mtx *sync.Mutex
}

func NewStatsMap() *StatsMap {
  sm := &StatsMap{make(map[string]interface{}), new(sync.Mutex)}
  return sm
}

func (sm *StatsMap) Max(key string, newValue int) int {
  var currValue int
  sm.mtx.Lock()
  currValueI, ok := sm.stats[key]
  if !ok {
    currValue = 0
  } else {
    currValue = currValueI.(int)
  }
  if currValue < newValue {
    currValue = newValue
  }
  sm.stats[key] = currValue
  sm.mtx.Unlock()
  return currValue
}

func (sm *StatsMap) IncrementStrInt(key string, idx int) int {
  var countMap map[string]int
  sm.mtx.Lock()
  count, ok := sm.stats[key]
  if !ok {
    countMap = make(map[string]int)
  } else {
    countMap = count.(map[string]int)
  }
  sm.stats[key] = countMap
  idxStr := strconv.Itoa(idx)
  c, ok := countMap[idxStr]
  if !ok {
    c = 0
  }
  c++
  countMap[idxStr] = c
  sm.mtx.Unlock()
  return c
}


func (sm *StatsMap) Add(key string, val int) int {
  var count int
  sm.mtx.Lock()
  countI, ok := sm.stats[key]
  if !ok {
    count = 0
  } else {
    count = countI.(int)
  }
  count += val
  sm.stats[key] = count
  sm.mtx.Unlock()
  return count
}

func (sm *StatsMap) Increment(key string) int {
  return sm.Add(key, 1)
}

func (sm *StatsMap) Export(file string) {
	jsonBytes, err := json.Marshal(sm.stats)
	if err != nil {
    log.Printf("Error marshaling StatsMap to json: %v.\n", err)
  }
  err = ioutil.WriteFile(file, jsonBytes, 0644)
  if err != nil {
    log.Printf("Error writing StatsMap json bytes to file %s: %v.\n",
      file, err)
  }
}
