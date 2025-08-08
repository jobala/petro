package util

type PetroError struct {
	Message string
	Err     error
}

func (e *PetroError) Error() string {
	return e.Message
}

func (e *PetroError) Unwrap() error {
	return e.Err
}

type BufferpoolExhaustedError struct {
	*PetroError
}
