package playground

// import "fmt"
// import "time"

var input = make([][]int, 10000000)

func SumOne() int {
	sum := 0
	for _, v := range input {
		sum += v[0]
	}
	return sum
}

func SumMany() int {
	sum1 := 0
	sum2 := 0
	sum3 := 0
	sum4 := 0
	for _, v := range input {
		sum1 += v[0]
		sum2 += v[1]
		sum3 += v[2]
		sum4 += v[3]
	}
	return sum1 + sum2 + sum3 + sum4
}

func sumUsingFunction() int {
	sum := 0
	for _, v := range input {
		sum += v[0]
	}
	return sum
}

func InitInput() {
	for i := 0; i < len(input); i++ {
		input[i] = make([]int, 5)
		// fmt.Println(input[i])
		for j := 0; j < len(input[i]); j++ {
			input[i][j] = j
		}
	}
}

func main() {
	InitInput()
	// fmt.Println(input[100])
	SumOne()
}
