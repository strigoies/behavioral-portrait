package com.yisa.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigInteger;
import java.util.*;

@Data
@NoArgsConstructor
public class HomeActivity {
    Map<Integer, BigInteger> dayActive = new HashMap<>();
    Integer occupation = 0;
    List<BigInteger> homePosition = new List<BigInteger>() {
        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean contains(Object o) {
            return false;
        }

        @Override
        public Iterator<BigInteger> iterator() {
            return null;
        }

        @Override
        public Object[] toArray() {
            return new Object[0];
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return null;
        }

        @Override
        public boolean add(BigInteger bigInteger) {
            return false;
        }

        @Override
        public boolean remove(Object o) {
            return false;
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return false;
        }

        @Override
        public boolean addAll(Collection<? extends BigInteger> c) {
            return false;
        }

        @Override
        public boolean addAll(int index, Collection<? extends BigInteger> c) {
            return false;
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return false;
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return false;
        }

        @Override
        public void clear() {

        }

        @Override
        public BigInteger get(int index) {
            return null;
        }

        @Override
        public BigInteger set(int index, BigInteger element) {
            return null;
        }

        @Override
        public void add(int index, BigInteger element) {

        }

        @Override
        public BigInteger remove(int index) {
            return null;
        }

        @Override
        public int indexOf(Object o) {
            return 0;
        }

        @Override
        public int lastIndexOf(Object o) {
            return 0;
        }

        @Override
        public ListIterator<BigInteger> listIterator() {
            return null;
        }

        @Override
        public ListIterator<BigInteger> listIterator(int index) {
            return null;
        }

        @Override
        public List<BigInteger> subList(int fromIndex, int toIndex) {
            return null;
        }
    };
}
