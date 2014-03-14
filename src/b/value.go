// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This file defines the value type used in this b-tree.

package b

type MetricWithCount struct {
	Count  int
	Metric []byte
}
