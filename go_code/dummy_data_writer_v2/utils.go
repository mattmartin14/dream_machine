package utils

import ("fmt"
		"rand"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func get_random_string(arr []string) string {
	rand_index := rand.Intn(len(arr))
	return arr[rand_index]
}

func get_random_date() string {
	lower_bound := time.Date(2020, 1, 1, 0, 0, 0, 0, time.Local)
	upper_bound := time.Date(2023, 1, 1, 0, 0, 0, 0, time.Local)

	time_span := upper_bound.Sub(lower_bound)

	rand_span := time.Duration(rand.Int63n(int64(time_span)))

	rand_dt := lower_bound.Add(rand_span)

	//note: the 2006-01-02 thing is significant for go lang; its when the time package was written....
	return rand_dt.Format("2006-01-02")
}