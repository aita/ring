package ring

import (
	"crypto/md5"
	"fmt"
	"sort"
)

type ketamaPoint struct {
	name string
	hash uint32
}

type Ketama struct {
	names  []string
	points []ketamaPoint
}

var _ Hash = (*Ketama)(nil)

func ketamaHash1(s string, replica int) uint32 {
	h := md5.New()
	digest := h.Sum([]byte(s))
	rd := replica * 4
	return uint32(digest[3+rd])<<24 | uint32(digest[2+rd])<<16 | uint32(digest[1+rd])<<8 | uint32(digest[0+rd])
}

func (ketama *Ketama) Set(servers map[string]Server) {
	var totalWeight float32
	names := make([]string, 0, len(servers))
	for name, server := range servers {
		totalWeight += float32(server.Weight)
		names = append(names, name)
	}

	numBuckets := len(servers)
	numPoints := numBuckets * 160
	points := make([]ketamaPoint, 0, numPoints)
	for name, server := range servers {
		pct := float32(server.Weight) / totalWeight
		limit := int(float32(float64(pct) * 40.0 * float64(numBuckets)))
		for k := 0; k < limit; k++ {
			ss := fmt.Sprintf("%s-%d", name, k)
			for i := 0; i < 4; i++ {
				points = append(points, ketamaPoint{
					name: name,
					hash: ketamaHash1(ss, i),
				})
			}
		}
	}
	sort.Slice(points, func(i, j int) bool {
		return points[i].hash < points[j].hash
	})

	ketama.names = names
	ketama.points = points
}

func (ketama *Ketama) Get(key string) string {
	h := ketamaHash1(key, 0)
	j := sort.Search(len(ketama.points), func(i int) bool {
		return ketama.points[i].hash >= h
	})
	if j == len(ketama.points) {
		j = 0
	}
	return ketama.points[j].name
}

func (ketama *Ketama) Members() []string {
	return ketama.names
}
