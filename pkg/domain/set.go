package domain

type OrderedSet[T comparable] struct {
	elements map[T]struct{} // For O(1) lookup
	order    []T            // For maintaining order
}

func NewOrderedSet[T comparable](elements ...T) *OrderedSet[T] {
	set := &OrderedSet[T]{
		elements: make(map[T]struct{}),
		order:    make([]T, 0),
	}
	set.Add(elements...)
	return set
}

func (s *OrderedSet[T]) Add(items ...T) {
	for _, item := range items {
		if _, exists := s.elements[item]; !exists {
			s.elements[item] = struct{}{}
			s.order = append(s.order, item)
		}
	}
}

func (s *OrderedSet[T]) Has(item T) bool {
	_, exists := s.elements[item]
	return exists
}

func (s *OrderedSet[T]) Delete(item T) {
	if _, exists := s.elements[item]; exists {
		delete(s.elements, item)
		// Remove from order slice
		for i, v := range s.order {
			if v == item {
				s.order = append(s.order[:i], s.order[i+1:]...)
				break
			}
		}
	}
}

// Size returns the number of elements in the set.
func (s *OrderedSet[T]) Size() int {
	return len(s.elements)
}

// ToSlice returns the elements in the order they were added.
func (s *OrderedSet[T]) ToSlice() []T {
	return s.order
}

type Set[T comparable] map[T]struct{}

// NewSet creates a new set from a slice of elements.
func NewSet[T comparable](elements ...T) Set[T] {
	s := make(Set[T])
	for _, e := range elements {
		s.Add(e)
	}
	return s
}

// Add adds an element to the set.
func (s Set[T]) Add(elements ...T) {
	for _, element := range elements {
		s[element] = struct{}{}
	}
}

// Has checks if an element exists in the set.
func (s Set[T]) Has(element T) bool {
	_, found := s[element]
	return found
}

// Delete removes an element from the set.
func (s Set[T]) Delete(element T) {
	delete(s, element)
}

// Size returns the number of elements in the set.
func (s Set[T]) Size() int {
	return len(s)
}

// ToSlice converts the set to a slice of its elements.
func (s Set[T]) ToSlice() []T {
	slice := make([]T, 0, len(s))
	for element := range s {
		slice = append(slice, element)
	}
	return slice
}
