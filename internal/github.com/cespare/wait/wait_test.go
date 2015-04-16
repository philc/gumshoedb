package wait

import (
	"fmt"
	"testing"
	"time"
)

func TestBasic(t *testing.T) {
	var w [5]int
	var wg Group
	for i := range w {
		i := i
		wg.Go(func(_ <-chan struct{}) error {
			w[i] = 1
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		t.Fatalf("expected nil err; got %v", err)
	}
	for _, v := range w {
		if v != 1 {
			t.Fatalf("want 1; got %v", v)
		}
	}
}

func TestErrReturn(t *testing.T) {
	var wg Group
	err := fmt.Errorf("boom")
	for i := 0; i < 5; i++ {
		i := i
		wg.Go(func(_ <-chan struct{}) error {
			if i == 2 {
				return err
			}
			return nil
		})
	}
	if err2 := wg.Wait(); err2 != err {
		t.Fatalf("want %v; got %v", err, err2)
	}
}

func TestQuit(t *testing.T) {
	var w [5]int
	var wg Group
	for i := range w {
		i := i
		wg.Go(func(quit <-chan struct{}) error {
			select {
			case <-time.After(10 * time.Duration(i) * time.Millisecond):
			case <-quit:
				return nil
			}
			w[i] = 1
			return nil
		})
	}
	time.Sleep(25 * time.Millisecond)
	wg.Quit()
	if err := wg.Wait(); err != nil {
		t.Fatalf("want nil err; got %v", err)
	}
	for i, v := range w {
		if i <= 2 && v != 1 {
			t.Fatalf("(i = %d) want 1; got %v", i, v)
		}
		if i > 2 && v != 0 {
			t.Fatalf("(i = %d) want 0; got %v", i, v)
		}
	}
}

func TestAutoQuit(t *testing.T) {
	var w [5]int
	var wg Group
	err := fmt.Errorf("boom")
	for i := range w {
		i := i
		wg.Go(func(quit <-chan struct{}) error {
			select {
			case <-time.After(10 * time.Duration(i) * time.Millisecond):
			case <-quit:
				return nil
			}
			w[i] = 1
			if i == 2 {
				return err
			}
			return nil
		})
	}
	if err2 := wg.Wait(); err2 != err {
		t.Fatalf("want %v; got %v", err, err2)
	}
	for i, v := range w {
		if i <= 2 && v != 1 {
			t.Fatalf("(i = %d) want 1; got %v", i, v)
		}
		if i > 2 && v != 0 {
			t.Fatalf("(i = %d) want 0; got %v", i, v)
		}
	}
}
