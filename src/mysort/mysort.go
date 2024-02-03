package mysort

/* From cmu mergesort, originally in python */
import (
	"genericsmr"
	"math"
	"mdlinproto"
  "state"
)

/*
def merge(a, start1, start2, end):
  index1 = start1
  index2 = start2
  length = end - start1
  aux = [None] * length
  for i in range(length):
      if ((index1 == start2) or
          ((index2 != end) and (a[index1] > a[index2]))):
          aux[i] = a[index2]
          index2 += 1
      else:
          aux[i] = a[index1]
          index1 += 1
  for i in range(start1, end):
      a[i] = aux[i - start1]
*/

func merge(a []*genericsmr.MDLPropose, start1 int, start2 int, end int) {
	index1 := start1
	index2 := start2
	length := end - start1
	aux := make([]*genericsmr.MDLPropose, length)
	var elem *genericsmr.MDLPropose
	for i := 0; i < length; i++ {
		if (index1 == start2) || ((index2 != end) && (a[index1].SeqNo < a[index2].SeqNo)) {
			elem = a[index2]
			index2 += 1
		} else {
			elem = a[index1]
			index1 += 1
		}
		mdlp := new(mdlinproto.Propose)
		mdlp.CommandId = elem.CommandId
		mdlp.Command = elem.Command
		//mdlp.Timestamp = elem.Timestamp
		mdlp.SeqNo = elem.SeqNo
		mdlp.PID = elem.PID
		aux[i] = &genericsmr.MDLPropose{mdlp, elem.Reply} // This is copying the bufio object :D
	}
	for i := start1; i < end; i++ {
		a[i] = aux[i-start1]
	}
}

/*
// Python version, from CMU's 15-112 course website
def mergeSort(a):
  n = len(a)
  step = 1
  while (step < n):
      for start1 in range(0, n, 2*step):
          start2 = min(start1 + step, n)
          end = min(start1 + 2*step, n)
          merge(a, start1, start2, end)
      step *= 2
*/

func MergeSort(a []*genericsmr.MDLPropose) {
	step := 1
	n := len(a)
	var start2 int
	var end int
	for step < n {
		for start1 := 0; start1 < n; start1 += 2 * step {
			start2 = int(math.Min(float64(start1+step), float64(n)))
			end = int(math.Min(float64(start1+2*step), float64(n)))
			merge(a, start1, start2, end)
		}
		step *= 2
	}
}

func mergeSecond(a []int64, b []state.Command, start1 int, start2 int, end int) {
	index1 := start1
	index2 := start2
	length := end - start1
	aux := make([]state.Command, length)
	for i := 0; i < length; i++ {
		if (index1 == start2) || ((index2 != end) && (a[index1] < a[index2])) {
			aux[i] = b[index2]
			index2 += 1
		} else {
			aux[i] = b[index1]
			index1 += 1
		}
	}
	for i := start1; i < end; i++ {
		b[i] = aux[i-start1]
	}
}

func EpochSort(a []int64, b []state.Command) {
  if (len(a) != len(b)) {
    panic("Cannot sort command list based on predecessor list of different length")
  }
  step := 1
  n := len(a)
  var start2 int
  var end int
  for step < n {
    for start1 := 0; start1 < n; start1 += 2 * step {
      start2 = int(math.Min(float64(start1+step), float64(n)))
      end = int(math.Min(float64(start1+2*step), float64(n)))
      mergeSecond(a, b, start1, start2, end)
    }
    step *= 2
  }
}
