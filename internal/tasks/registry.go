package tasks

import "fmt"

// Factory creates an empty Task of one type, ready for Task.Load.
type Factory func() Task

// Registry maps task type names to factories. Register every type at
// startup; after that, a Registry is safe for concurrent use.
type Registry struct {
	factories map[string]Factory
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{factories: make(map[string]Factory)}
}

// Register adds a task type.
//
// Parameters:
//   - taskType: the name stored in the task_type column.
//   - f: creates an empty Task of that type.
//
// It panics if taskType is empty or already registered: both are programming
// errors that should stop the program at startup.
func (r *Registry) Register(taskType string, f Factory) {
	if taskType == "" || f == nil {
		panic("tasks: Register with empty type or nil factory")
	}
	if _, dup := r.factories[taskType]; dup {
		panic(fmt.Sprintf("tasks: task type %q registered twice", taskType))
	}
	r.factories[taskType] = f
}

// New creates an empty Task of the given type.
//
// Returns a NonRetriable error if the type is not registered: retrying on
// the same worker would not help.
func (r *Registry) New(taskType string) (Task, error) {
	f, ok := r.factories[taskType]
	if !ok {
		return nil, NonRetriable(fmt.Errorf("unknown task type %q", taskType))
	}
	return f(), nil
}
