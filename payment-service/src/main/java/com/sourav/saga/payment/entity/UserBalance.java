package com.sourav.saga.payment.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Entity
@Table(name = "T_USER_BALANCE")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserBalance {
    @Id
    private int userId;
    private int price;
}
