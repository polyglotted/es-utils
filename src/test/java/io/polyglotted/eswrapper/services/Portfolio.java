package io.polyglotted.eswrapper.services;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

@EqualsAndHashCode(doNotUseGetters = true)
@RequiredArgsConstructor
public class Portfolio {
    public static final String PORTFOLIO_TYPE = "Portfolio";
    public static final String FieldAddress = "address";

    public final String address;
    public final String name;
}
