from typing import TypeVar, Generic, Callable, Union, cast, Optional
from dataclasses import dataclass


T = TypeVar('T')
E = TypeVar('E')


class ResultError(Exception):
    pass


@dataclass
class Result(Generic[T, E]):
    """A Result type that can contain either a success value of type T or an error value of type E"""
    _is_ok: bool
    _value: Union[T, E]

    @staticmethod
    def Ok(value: T) -> 'Result[T, E]':
        """Create a success Result containing the given value"""
        return Result(True, value)

    @staticmethod
    def Err(error: E) -> 'Result[T, E]':
        """Create an error Result containing the given error"""
        return Result(False, error)

    def is_ok(self) -> bool:
        """Returns True if the Result is a success value"""
        return self._is_ok

    def is_err(self) -> bool:
        """Returns True if the Result is an error value"""
        return not self._is_ok

    def ok(self) -> Optional[T]:
        """Returns the success value if present, otherwise None"""

        if self._is_ok:
            return cast(T, self._value)
            
        return None


    def err(self) -> Union[E, None]:
        """Returns the error value if present, otherwise None"""
        if not self._is_ok:
            return cast(E, self._value)

        return None

    def unwrap(self) -> T:
        """Returns the success value if present, otherwise raises ResultError"""
        if self._is_ok:
            return cast(T, self._value)
        
        raise ResultError(f"Called unwrap on an Err value: {self._value}")

    def unwrap_or(self, default: T) -> T:
        """Returns the success value if present, otherwise returns the default"""
        if self._is_ok:
            return cast(T, self._value)
        
        return default

    def unwrap_or_else(self, op: Callable[[E], T]) -> T:
        """Returns the success value if present, otherwise returns the result of calling op with the error"""
        if self._is_ok:
            return cast(T, self._value)
        
        return op(cast(E, self._value))

    def expect(self, msg: str) -> T:
        """Returns the success value if present, otherwise raises ResultError with the given message"""
        if self._is_ok:
            return cast(T, self._value)
        
        raise ResultError(f"{msg}: {self._value}")

    def map(self, op: Callable[[T], T]) -> 'Result[T, E]':
        """Applies the given function to the success value if present"""
        if self._is_ok:
            return Result.Ok(op(cast(T, self._value)))
        
        return self

    def map_err(self, op: Callable[[E], E]) -> 'Result[T, E]':
        """Applies the given function to the error value if present"""
        if not self._is_ok:
            return Result.Err(op(cast(E, self._value)))
        
        return self

    def and_then(self, op: Callable[[T], 'Result[T, E]']) -> 'Result[T, E]':
        """Returns the result of applying op to the success value if present"""
        if self._is_ok:
            return op(cast(T, self._value))
        
        return self