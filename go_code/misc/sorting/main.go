package main

import "fmt"

func bubble_sort(arr []int) {

	n := len(arr)

	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if arr[j] > arr[j+1] {
				arr[j], arr[j+1] = arr[j+1], arr[j]
			}
		}
	}
}

func main() {

	arr := []int{3, 8, 23, 12, 6, 2, 7, 4, 1}

	bubble_sort(arr)
	fmt.Printf("Sorted array is: %v\n", arr)
}
