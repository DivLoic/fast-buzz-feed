package com.pubsap.eng.streams;

import com.pubsap.eng.schema.*;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FizzBuzzAggregatorTest {

    @Test
    public void aggregatorShouldUpdateFizz() {
        //Given
        InputKey givenInputKey = new InputKey("client-1");
        Item givenItem = new Item(ItemValue.Fizz);
        Output givenInitOutput = new Output(1,0,0);

        //when
        Output aggResult = FizzBuzzAggregator.aggregator.apply(givenInputKey, givenItem, givenInitOutput);

        //Then
        assertEquals(aggResult,new Output(2,0,0));
    }

    @Test
    public void aggregatorShouldUpdateFizzFromInit() {
        //Given
        InputKey givenInputKey = new InputKey("client-1");
        Item givenItem = new Item(ItemValue.Fizz);
        Output givenInitOutput = FizzBuzzAggregator.init.apply();

        //when
        Output aggResult = FizzBuzzAggregator.aggregator.apply(givenInputKey, givenItem, givenInitOutput);

        //Then
        assertEquals(aggResult,new Output(1,0,0));
    }

    @Test
    public void aggregatorShouldUpdateBuzz() {
        //Given
        InputKey givenInputKey = new InputKey("client-1");
        Item givenItem = new Item(ItemValue.Buzz);
        Output givenInitOutput = new Output(0,1,0);

        //when
        Output aggResult = FizzBuzzAggregator.aggregator.apply(givenInputKey, givenItem, givenInitOutput);

        //Then
        assertEquals(aggResult,new Output(0,2,0));
    }

    @Test
    public void aggregatorShouldUpdateBuzzFromInit() {
        //Given
        InputKey givenInputKey = new InputKey("client-1");
        Item givenItem = new Item(ItemValue.Buzz);
        Output givenInitOutput = FizzBuzzAggregator.init.apply();

        //when
        Output aggResult = FizzBuzzAggregator.aggregator.apply(givenInputKey, givenItem, givenInitOutput);

        //Then
        assertEquals(aggResult,new Output(0,1,0));
    }

    @Test
    public void aggregatorShouldUpdateFizzBuzz() {
        //Given
        InputKey givenInputKey = new InputKey("client-1");
        Item givenItem = new Item(ItemValue.FizzBuzz);
        Output givenInitOutput = new Output(0,0,1);

        //when
        Output aggResult = FizzBuzzAggregator.aggregator.apply(givenInputKey, givenItem, givenInitOutput);

        //Then
        assertEquals(aggResult,new Output(0,0,2));
    }

    @Test
    public void aggregatorShouldUpdateFizzBuzzFromInit() {
        //Given
        InputKey givenInputKey = new InputKey("client-1");
        Item givenItem = new Item(ItemValue.FizzBuzz);
        Output givenInitOutput = FizzBuzzAggregator.init.apply();

        //when
        Output aggResult = FizzBuzzAggregator.aggregator.apply(givenInputKey, givenItem, givenInitOutput);

        //Then
        assertEquals(aggResult,new Output(0,0,1));
    }

    @Test
    public void aggregatorInitShouldGetNeutralElementOfSum() {

        Output givenInitOutput = FizzBuzzAggregator.init.apply();

        //Then
        assertEquals(givenInitOutput,new Output(0,0,0));
    }
}
