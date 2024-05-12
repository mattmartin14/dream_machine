package main

import (
	"fmt"
	"image/color"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

// this draws a green triangle and saves it as a png

func main() {
	// Create a new plot
	p := plot.New()

	// Define the vertices of the triangle
	vertices := triangle()

	poly, err := plotter.NewPolygon(vertices)
	if err != nil {
		panic(err)
	}
	poly.Color = color.RGBA{R: 0, G: 255, A: 255} // Set color to green

	// Add the polygon plot to the plot
	p.Add(poly)

	// Save the plot to a PNG file
	if err := p.Save(4*vg.Inch, 4*vg.Inch, "triangle.png"); err != nil {
		panic(err)
	}

	fmt.Println("Triangle plotted and saved as triangle.png")
}

// Function to define the vertices of the triangle
func triangle() plotter.XYs {
	return plotter.XYs{
		{0, 0},
		{5, 0},
		{2.5, 4.33}, // Coordinates adjusted for equilateral triangle
		{0, 0},      // To close the triangle
	}
}
